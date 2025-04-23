package message;

import connection.ClientConnection;
import communication.CommunicationStrategy;
import server.Server;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class ServerMessageHandler implements MessageHandler {
    private final Server server;
    private final CommunicationStrategy strategy;


    public ServerMessageHandler(Server server, CommunicationStrategy strategy) {
        this.server = server;
        this.strategy = strategy;
    }

    @Override
    public void handleMessage(String message, ClientConnection connection) {
        String[] parts = message.split("\\|");
        String command = parts[0];

        switch (command) {
            case "LEADER":
                int receivedLeaderId = Integer.parseInt(parts[1]);
                server.setLeaderId(receivedLeaderId);
                System.out.println("[SERVER] Líder atual definido como: " + receivedLeaderId);
                break;

            case "VECTOR":
                if(server.isLeader()){
                    String vector = parts[1];
                    String followers = parts[2];
                    String requestId = parts[3];
                    System.out.println("[LEADER] RECEBI O VECTOR " + vector);
                    server.handleVectorAsLeader(vector, followers, requestId);
                    server.startResultTimeout(requestId, strategy);
                }else {
                    String vector = parts[1].trim();
                    String leaderIdString = parts[2].trim();
                    String requestId = parts[3];
                    System.out.println("[FOLLOWERS] RECEBI O VECTOR " + vector);
                    int leaderId = Integer.parseInt(leaderIdString);
                    server.handleVectorAsFollower(vector, leaderId, requestId);
                }
                break;
            case "RESULT":
                int partial = Integer.parseInt(parts[1]);
                String requestId = parts[2];
                System.out.println("[LEADER] RESULT: " + partial);

                server.addPartialResult(requestId, partial);

                if (server.allResultsReceived(requestId)) {
                    int total = server.getTotalResult(requestId);
                    System.out.println("[LEADER] Resultado final: " + total);

                    strategy.sendMessage("RESULT|" + total + "|" + requestId, 0);

                    server.clearResults(requestId);
                }
                break;
            case "SET_FOLLOWERS":
                if (parts.length < 2 || parts[1].isEmpty()) {
                    System.out.println("[SERVER] Nenhum follower recebido.");
                    break;
                }
                String[] ids = parts[1].split(",");
                for (String idStr : ids) {
                    int id = Integer.parseInt(idStr);
                    if (id != server.getServerId()) {
                        int port = 9000 + id;
                        InetSocketAddress address = new InetSocketAddress("localhost", port);
                        strategy.addNodeAddress(id, address);
                        System.out.println("[LEADER] Adicionado follower ID=" + id + " (localhost:" + port + ")");
                    }
                }
                server.setFollowers(Arrays.stream(ids).map(Integer::parseInt).toList());
                break;

            default:
                System.out.println("[SERVER] Tipo de mensagem não reconhecido: " + command);
                break;
    }
}
}

