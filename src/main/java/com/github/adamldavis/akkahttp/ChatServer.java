package com.github.adamldavis.akkahttp;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.japi.JavaPartialFunction;
import akka.stream.*;
import akka.stream.javadsl.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ChatServer {

    static final int BUFFER_SIZE = 1;
    private final ActorSystem actorSystem;
    private final MessageRepository messageRepository = new MessageRepository();
    private final ActorMaterializer materializer;

    private final Source<ChatMessage, NotUsed> source;
    private final Sink<ChatMessage, NotUsed> sink;
    private final Publisher<ChatMessage> publisher;
    private final int parallelism;

    public ChatServer(ActorSystem actorSystem) {
        parallelism = Runtime.getRuntime().availableProcessors();
        this.actorSystem = actorSystem;
        materializer = ActorMaterializer.create(actorSystem);
        var asPublisher = Sink.<ChatMessage>asPublisher(AsPublisher.WITH_FANOUT);
        var publisherSinkPair = asPublisher.preMaterialize(materializer);

        publisher = publisherSinkPair.first();
        sink = publisherSinkPair.second();
        source = Source.fromPublisher(publisher);
    }

    public Publisher<ChatMessage> getPublisher() {
        return publisher;
    }

    private Flow<String, ChatMessage, NotUsed> parseContent() {
        return Flow.of(String.class).map(line -> line.split("\\|"))
                .map(fields -> new ChatMessage(fields[0], fields[1]));
    }

    private Sink<ChatMessage, CompletionStage<ChatMessage>> storeChatMessages() {
        return Flow.of(ChatMessage.class)
                .mapAsyncUnordered(parallelism, messageRepository::save)
                .toMat(Sink.last(), Keep.right());
    }


    CompletionStage<ChatMessage> storeMessageFromContent(CompletionStage<String> content) {
        return Source.fromCompletionStage(content)
                .via(parseContent())
                .runWith(storeChatMessages(), materializer)
                .whenComplete((message, ex) -> {
                    if (message != null) {
                        System.out.println("Saved message: " + message);
                    } else {
                        ex.printStackTrace();
                    }
                });
    }

    public Flow<Message, Message, NotUsed> flow() {
        /* JavaPartialFunction: Helper for implementing a *pure* partial function: it will possibly be invoked multiple
        times for a single 'application', because its only abstract method is used for both isDefinedAt()
        and apply(); the former is mapped to isCheck == true and the latter to isCheck == false for those cases
        where this is important to know. */
        Flow<Message, ChatMessage, NotUsed> savingFlow = Flow.<Message>create()
                .buffer(BUFFER_SIZE, OverflowStrategy.backpressure())
                .collect(new JavaPartialFunction<Message, CompletionStage<ChatMessage>>() {
                    @Override
                    public CompletionStage<ChatMessage> apply(Message msg, boolean isCheck) {
                        if (isCheck) {
                            if (msg.isText()) return null;
                            else throw noMatch();
                        } else {
                            TextMessage textMessage = msg.asTextMessage();
                            if (textMessage.isStrict()) {
                                return storeMessageFromContent(
                                        CompletableFuture.completedFuture(textMessage.getStrictText()));

                            } else {
                                return storeMessageFromContent(textMessage.getStreamedText()
                                        .runFold("", (each, total) -> total + each, materializer));
                            }
                        }
                    }
                }).mapAsync(parallelism, stage -> stage); // unwraps the CompletionStage

        final Graph<FlowShape<Message, Message>, NotUsed> graph =
                GraphDSL.create(builder -> {
                    final FlowShape<ChatMessage, Message> toMessage =
                            builder.add(Flow.of(ChatMessage.class).map(ChatMessage::toString).async()
                                    .map(TextMessage::create));

                    final UniformFanInShape<Message, Message> merge = builder.add(Merge.create(1));

                    Inlet<ChatMessage> sinkInlet = builder.add(sink).in();
                    Outlet<ChatMessage> sourceOutlet = builder.add(source).out();
                    FlowShape<Message, ChatMessage> saveFlow = builder.add(savingFlow);

                    builder.from(saveFlow.out()).toInlet(sinkInlet); // connect saveFlow to global sink
                    builder.from(sourceOutlet).via(toMessage).toInlet(merge.in(0)); // convert source and send to M

                    return new FlowShape<>(saveFlow.in(), merge.out()); // define FlowShape
                });
        return Flow.fromGraph(graph);
    }

    public int getParallelism() {
        return parallelism;
    }
}
