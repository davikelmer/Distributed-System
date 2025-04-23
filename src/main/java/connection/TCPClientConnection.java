package connection;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import java.util.concurrent.locks.ReentrantLock;

public class TCPClientConnection  implements ClientConnection {

    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;
    private final ReentrantLock writeLock = new ReentrantLock();

    public TCPClientConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    @Override
    public BufferedReader getIn() {
        return in;
    }

    @Override
    public BufferedWriter getOut() {
        return out;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public void sendMessage(String message) throws IOException {
        writeLock.lock();
        try {
            out.write(message);
            out.newLine();
            out.flush();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException ignored) {}
        try {
            out.close();
        } catch (IOException ignored) {}
        try {
            socket.close();
        } catch (IOException ignored) {}
    }
}

