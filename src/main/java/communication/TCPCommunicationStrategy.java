package communication;

import connection.ClientConnection;
import connection.TCPClientConnection;
import message.MessageHandler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TCPCommunicationStrategy implements CommunicationStrategy {

    private final ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses;
    private final ConcurrentHashMap<Integer, ClientConnection> activeConnections;
    private final ExecutorService executorService;
    private ServerSocket serverSocket;
    private final int ownPort;
    MessageHandler messageHandler;


    public TCPCommunicationStrategy(ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses, int port) {
        this.nodeAddresses = nodeAddresses;
        this.activeConnections = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
        this.ownPort = port;
    }

    @Override
    public void startListening(int ownPort, MessageHandler messageHandler) {
        this.messageHandler = messageHandler;

        executorService.submit(() -> {
            try {
                serverSocket = new ServerSocket(getOwnPort());
                System.out.println("Servidor TCP iniciado na porta " + getOwnPort());

                while (!Thread.currentThread().isInterrupted()) {
                    Socket socket = serverSocket.accept();
                    executorService.submit(() -> handleIncomingConnection(socket));
                }
            } catch (IOException e) {
                System.err.println("Erro ao iniciar servidor TCP: " + e.getMessage());
            } finally {
                try {
                    if (serverSocket != null && !serverSocket.isClosed()) {
                        serverSocket.close();
                        System.out.println("[TCP] ServerSocket fechado.");
                    }
                } catch (IOException e) {
                    System.err.println("Erro ao fechar serverSocket: " + e.getMessage());
                }
            }
        });
    }

    private void handleIncomingConnection(Socket socket) {
        TCPClientConnection connection = null;
        int remoteId = -1;

        try {
            connection = new TCPClientConnection(socket);
            remoteId = ((InetSocketAddress) socket.getRemoteSocketAddress()).getPort();

            activeConnections.putIfAbsent(remoteId, connection);
            System.out.println("[TCP] Conexão estabelecida com " + remoteId);

            BufferedReader in = connection.getIn();

            while (!Thread.currentThread().isInterrupted()) {
                String message = in.readLine();
                if (message == null) {
                    break;
                }

                System.out.println("[TCP] Mensagem recebida de " + remoteId + ": " + message);
                messageHandler.handleMessage(message, connection);
            }

        } catch (IOException e) {
            System.err.println("[TCP] Erro na conexao com o ID " + remoteId + ": " + e.getMessage());
        } finally {
            if (connection != null) {
                activeConnections.remove(remoteId);
                try {
                    connection.getSocket().close();
                    System.out.println("[TCP] Conexao com " + remoteId + " fechada.");
                } catch (IOException e) {
                    System.err.println("[TCP] Erro ao fechar conexão com " + remoteId + ": " + e.getMessage());
                }
            }
        }
    }


    @Override
    public void sendMessage(String messageStr, int targetId) {
        executorService.submit(() -> {
            try {
                TCPClientConnection connection = (TCPClientConnection) getOrCreateConnection(targetId);

                if (connection.getSocket().isClosed() || !connection.getSocket().isConnected()) {
                    System.err.println("[TCP] A conexao com o node " + targetId + " nao esta ativa.");
                    return;
                }

                BufferedWriter out = connection.getOut();
                System.out.println("[TCP] Enviando mensagem para " + targetId + ": " + messageStr);

                synchronized (out) {
                    out.write(messageStr);
                    out.newLine();
                    out.flush();
                }
            } catch (IOException e) {
                System.err.println("Erro ao enviar mensagem para " + targetId + ": " + e.getMessage());
                activeConnections.remove(targetId);
            }
        });
    }


    private ClientConnection getOrCreateConnection(int targetId) throws IOException {
            TCPClientConnection connection = (TCPClientConnection) activeConnections.get(targetId);
        if (connection != null && !connection.getSocket().isClosed() && connection.getSocket().isConnected()) {
            return connection;
        }
        InetSocketAddress address = nodeAddresses.get(targetId);
        if (address == null) {
            throw new IOException("Endereco não encontrado para o no " + targetId);
        }

        Socket socket = new Socket();
        socket.connect(address, 5000);
        ClientConnection newConnection = new TCPClientConnection(socket);
        activeConnections.put(targetId, newConnection);
        return newConnection;
    }


    private int getOwnPort() {
        return ownPort;
    }
    @Override
    public void addNodeAddress(int id, InetSocketAddress address) {
            nodeAddresses.put(id, address);
    }


}


