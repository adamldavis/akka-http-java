package com.github.adamldavis.akkhttp;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.stream.ActorMaterializer;
import akka.stream.Graph;
import akka.stream.SinkShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.*;
import akka.stream.javadsl.Flow;
import akka.testkit.javadsl.TestKit;
import com.github.adamldavis.akkahttp.ChatServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ChatServerTest {

    ChatServer chatServer;
    ActorSystem actorSystem;
    ActorMaterializer materializer;

    @Before
    public void setup() {
        actorSystem = ActorSystem.create("test-system");
        chatServer = new ChatServer(actorSystem);
        materializer = ActorMaterializer.create(actorSystem);
    }
    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(actorSystem);
    }

    @Test
    public void flow_should_exist(){
        assertThat(chatServer.flow()).isNotNull();
    }

    @Test
    public void flow_should_copy_messages() throws ExecutionException, InterruptedException {
        final Collection<Message> list = new ConcurrentLinkedDeque<>();
        Flow<Message, Message, NotUsed> flow = chatServer.flow();

        assertThat(flow).isNotNull();

        List<Message> messages = Arrays.asList(TextMessage.create("foo|bar"));
        Graph<SourceShape<Message>, ?> testSource = Source.from(messages);
        Graph<SinkShape<Message>, CompletionStage<Done>> testSink = Sink.foreach(list::add);

        CompletionStage<Done> results = flow.runWith(testSource, testSink, materializer).second();
        try {
            results.toCompletableFuture().get(2, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            System.out.println("caught expected: " + te.getMessage());
        }

        Iterator<Message> iterator = list.iterator();
        assertThat(list.size()).isEqualTo(1);
        assertThat(iterator.next().asTextMessage().getStrictText()).isEqualTo("ChatMessage {username='foo', message='bar'}");
    }

    @Test
    public void flow_should_copy_many_messages() throws ExecutionException, InterruptedException {
        System.out.println("parallelism=" + chatServer.getParallelism());
        final Collection<Message> list = new ConcurrentLinkedDeque<>();
        Graph<SourceShape<Message>, ?> testSource = Source.range(1, 100).map(i -> TextMessage.create("foo|bar" + i));
        Graph<SinkShape<Message>, CompletionStage<Done>> testSink = Sink.foreach(list::add);
        Flow<Message, Message, NotUsed> flow = chatServer.flow();
        assertThat(flow).isNotNull();
        CompletionStage<Done> results = flow.runWith(testSource, testSink, materializer).second();
        // this would go one forever, so we have to timeout:
        try {
            results.toCompletableFuture().get(25, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            System.out.println("caught expected: " + te.getMessage());
        }
        assertThat(list.size()).isEqualTo(100);

        list.stream().map(m -> (TextMessage) m)
                .map(TextMessage::getStrictText).forEach(System.out::println);
    }
}
