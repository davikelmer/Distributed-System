package connection;


import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class HTTPClientConnection implements ClientConnection {

    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;

    public HTTPClientConnection(Socket socket, BufferedReader in, BufferedWriter out) throws IOException {
        this.socket = socket;
        this.in = in;
        this.out = out;
    }

    @Override
    public void sendMessage(String message) throws IOException {
        try {
            String httpResponse = "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: text/plain\r\n" +
                    "Content-Length: " + message.length() + "\r\n" +
                    "\r\n" +
                    message;

            out.write(httpResponse);
            out.flush();
        } catch (IOException e) {
            System.err.println("[HTTP] Erro ao enviar resposta: " + e.getMessage());
        } finally {
            try {
                out.close();
                in.close();
                socket.close();
            } catch (IOException e) {
                System.err.println("[HTTP] Erro ao fechar conexões: " + e.getMessage());
            }
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public Object getSocket() {
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
        return new InetSocketAddress(socket.getInetAddress(), socket.getPort());
    }
}