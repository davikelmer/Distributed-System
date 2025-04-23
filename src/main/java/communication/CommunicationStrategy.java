package communication;

import message.MessageHandler;

import java.net.InetSocketAddress;

public interface CommunicationStrategy {
    void startListening(int ownport, MessageHandler messageHandler);
    void sendMessage(String message, int targetId);
    void addNodeAddress(int id, InetSocketAddress address);
}

