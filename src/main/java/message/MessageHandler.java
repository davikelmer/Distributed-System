package message;

import connection.ClientConnection;

public interface MessageHandler {
    void handleMessage(String message, ClientConnection connection);
}
