package com.github.adamldavis.akkahttp;

public class ChatMessage {

    final String username;
    final String message;

    public ChatMessage(String username, String message) {
        this.username = username;
        this.message = message;
    }

    @Override
    public String toString() {
        return "ChatMessage {username='" + username + '\'' +
                ", message='" + message + '\'' + '}';
    }
}
