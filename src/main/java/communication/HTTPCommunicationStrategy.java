package communication;

import connection.HTTPClientConnection;
import message.MessageHandler;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class HTTPCommunicationStrategy implements CommunicationStrategy {

    private final ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses;
    private final int ownPort;
    private MessageHandler messageHandler;
    private final ExecutorService executorService;

    public HTTPCommunicationStrategy(ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses, int port) {
        this.nodeAddresses = nodeAddresses;
        this.ownPort = port;
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void startListening(int ownPort, MessageHandler handler) {
        this.messageHandler = handler;

        executorService.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(ownPort)) {
                System.out.println("[HTTP] Servidor HTTP iniciado na porta " + ownPort);

                while (!Thread.currentThread().isInterrupted()) {
                    Socket clientSocket = serverSocket.accept();

                    executorService.submit(() -> {
                        try {
                            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

                            String line;
                            int contentLength = 0;
                            while ((line = in.readLine()) != null && !line.isEmpty()) {
                                if (line.toLowerCase().startsWith("content-length:")) {
                                    contentLength = Integer.parseInt(line.split(":")[1].trim());
                                }
                            }

                            char[] bodyChars = new char[contentLength];
                            in.read(bodyChars);
                            String message = new String(bodyChars).trim();

                            System.out.println("[HTTP] Mensagem recebida: " + message);

                            HTTPClientConnection connection = new HTTPClientConnection(clientSocket, in, out);
                            messageHandler.handleMessage(message, connection);

                        } catch (IOException e) {
                            System.err.println("[HTTP] Erro ao lidar com requisição: " + e.getMessage());
                        }
                    });
                }

            } catch (IOException e) {
                System.err.println("[HTTP] Erro no servidor HTTP: " + e.getMessage());
            }
        });
    }

    @Override
    public void sendMessage(String messageStr, int targetId) {
        executorService.submit(() -> {
            Socket socket = null;
            try {
                InetSocketAddress address = nodeAddresses.get(targetId);
                if (address == null) {
                    System.err.println("[HTTP] Endereço não encontrado para o ID " + targetId);
                    return;
                }

                socket = new Socket();
                socket.setReuseAddress(true);
                socket.setSoLinger(true, 0);
                socket.connect(new InetSocketAddress(address.getAddress(), address.getPort()), 2000);

                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String httpRequest =
                        "POST /message HTTP/1.1\r\n" +
                                "Host: " + address.getHostName() + "\r\n" +
                                "Content-Type: text/plain\r\n" +
                                "Content-Length: " + messageStr.length() + "\r\n" +
                                "\r\n" +
                                messageStr;

                out.write(httpRequest);
                out.flush();

                String line;
                while ((line = in.readLine()) != null && !line.isEmpty()) {
                }

                char[] buffer = new char[1024];
                while (in.ready()) {
                    in.read(buffer);
                }

                System.out.println("[HTTP] Enviada mensagem para " + address + ": " + messageStr);

            } catch (IOException e) {
                System.err.println("[HTTP] Erro ao enviar mensagem para ID " + targetId + ": " + e.getMessage());
            } finally {
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                        System.out.println("[HTTP] Socket fechado com sucesso");
                    } catch (IOException e) {
                        System.err.println("[HTTP] Erro ao fechar socket: " + e.getMessage());
                    }
                }
            }
        });
    }



    @Override
    public void addNodeAddress(int id, InetSocketAddress address) {
        nodeAddresses.put(id, address);
    }
}