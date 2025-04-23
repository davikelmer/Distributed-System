package connection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;

public interface ClientConnection {
    void sendMessage(String message) throws IOException;
    void close() throws IOException;
    Object getSocket();
    BufferedReader getIn();
    BufferedWriter getOut();
    InetSocketAddress getRemoteAddress();

}
