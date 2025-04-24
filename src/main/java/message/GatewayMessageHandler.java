package message;

import connection.ClientConnection;
import communication.CommunicationStrategy;
import gateway.ApiGateway;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


public class GatewayMessageHandler implements MessageHandler {
    private final ApiGateway gateway;
    private final CommunicationStrategy strategy;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final Map<String, ClientConnection> requestToClientConnection = new ConcurrentHashMap<>();

    public GatewayMessageHandler(ApiGateway gateway, CommunicationStrategy strategy) {
        this.gateway = gateway;
        this.strategy = strategy;
    }

    @Override
    public void handleMessage(String message, ClientConnection connection) {
        System.out.println("[GATEWAY] Mensagem recebida: " + message);

        try {
            String[] parts = message.split("\\|");

            if (parts.length < 2) {
                System.out.println("[GATEWAY] Mensagem inválida: " + message);
                return;
            }

            String command = parts[0];

            switch (command) {
                case "HEARTBEAT":
                    int serverId = Integer.parseInt(parts[1]);
                    gateway.handleHeartbeat(serverId);
                    break;
                case "VECTOR":
                    String vector = parts[1];
                    String requestId = UUID.randomUUID().toString();

                    String clientId = "UNKNOWN";
                    Object socketObj = connection.getSocket();
                    if (socketObj instanceof Socket socket) {
                        clientId = socket.getRemoteSocketAddress().toString();
                    } else if (socketObj instanceof DatagramSocket) {
                        if (connection.getRemoteAddress() != null) {
                            clientId = connection.getRemoteAddress().toString();
                        }
                    }

                    requestToClientConnection.put(requestId, connection);

                    System.out.println("[GATEWAY] Recebido vetor de " + clientId + " com requestId " + requestId);
                    gateway.handleVector(vector, requestId);
                    scheduler.schedule(() -> {
                        ClientConnection conn = requestToClientConnection.remove(requestId);
                        if (conn != null) {
                            try {
                                conn.sendMessage("ERROR|Timeout enquanto esperava por resultado.");
                                System.out.println("[GATEWAY] Timeout: resposta não recebida para requestId " + requestId);
                            } catch (IOException e) {
                                System.err.println("[GATEWAY] Erro ao enviar mensagem de timeout ao cliente.");
                                e.printStackTrace();
                            }
                        }
                    }, 10, TimeUnit.SECONDS);
                    break;
                case "RESULT":
                    if (parts.length < 3) {
                        System.err.println("[GATEWAY] Mensagem RESULT malformada: " + message);
                        break;
                    }

                    String result = parts[1];
                    String requestId2 = parts[2];

                    ClientConnection clientConn = requestToClientConnection.remove(requestId2);

                    if (clientConn != null) {
                        try {
                            clientConn.sendMessage("RESULT|" + result);
                            System.out.println("[GATEWAY] Resultado enviado ao cliente (req " + requestId2 + "): " + result);
                        } catch (IOException e) {
                            System.err.println("[GATEWAY] Erro ao enviar resultado ao cliente: " + requestId2);
                            e.printStackTrace();
                        }
                    } else {
                        System.err.println("[GATEWAY] Nenhuma conexão encontrada para requestId: " + requestId2);
                    }
                    InetSocketAddress repositoryAddress = gateway.getRepositoryAddress();
                    message = "SAVE|" + result + "|" + requestId2;
                    strategy.sendMessage(message, repositoryAddress.getPort()-9000);
                    break;
                default:
                    System.out.println("[GATEWAY] Tipo de mensagem não reconhecido: " + command);
                    break;
            }

        } catch (Exception e) {
            System.out.println("[GATEWAY] Erro ao processar mensagem: " + message);
            e.printStackTrace();
        }
    }
}
