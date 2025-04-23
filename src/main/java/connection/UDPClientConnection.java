package connection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class UDPClientConnection implements ClientConnection {
    private final DatagramSocket socket;
    private final SocketAddress address;

    public UDPClientConnection(DatagramSocket socket, SocketAddress address) {
        this.socket = socket;
        this.address = address;
    }

    @Override
    public void sendMessage(String message) throws IOException {
        byte[] data = message.getBytes();
        DatagramPacket packet = new DatagramPacket(data, data.length, address);
        socket.send(packet);
    }

    public SocketAddress getAddress() {
        return address;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public DatagramSocket getSocket() {
        return socket;
    }

    @Override
    public BufferedReader getIn() {
        return null;
    }

    @Override
    public BufferedWriter getOut() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) address;
    }
}
