package server;

import communication.CommunicationStrategy;
import message.ServerMessageHandler;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class Server {
    private final int port;
    private final int serverId;
    private final CommunicationStrategy strategy;
    private final ServerMessageHandler messageHandler;
    private int leaderId = -1;
    private final ExecutorService executorService;
    private List<Integer> followers = new ArrayList<>();
    private final ConcurrentHashMap<String, List<Integer>> partialResults = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    public Server(int port, CommunicationStrategy strategy) {
        this.port = port;
        this.serverId = port - 9000;
        this.strategy = strategy;
        this.messageHandler = new ServerMessageHandler(this, strategy);
        this.executorService = Executors.newCachedThreadPool();
    }

    public void start() {
        executorService.submit(() ->
                strategy.startListening(port, new ServerMessageHandler(this, strategy)));

        startSendingHeartbeats();
    }


    private void startSendingHeartbeats() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                String heartbeatMessage = "HEARTBEAT|" + serverId;
                strategy.sendMessage(heartbeatMessage, 0);
                System.out.println("[SERVER " + serverId + "] Heartbeat enviado para o gateway");
            }
        }, 0, 5000);
    }

    public int getServerId() {
        return serverId;
    }

    public void setLeaderId(int receivedLeaderId) {
        this.leaderId = receivedLeaderId;
    }

    public boolean isLeader() {
        return this.serverId == this.leaderId;
    }

    public void handleVectorAsLeader(String vector, String followers, String requestId) {
        System.out.println("[LEADER] Vetor recebido pelo lider");
        String[] vectorParts = vector.split(",");
        String[] followersIds = followers.split(",");

        int divisionSize = vectorParts.length / followersIds.length;
        int remainder = vectorParts.length % followersIds.length;

        int startIndex = 0;

        for (int i = 0; i < followersIds.length; i++) {
            int currentDivisionSize = divisionSize + (i < remainder ? 1 : 0);
            int endIndex = startIndex + currentDivisionSize;

            String division = String.join(",", Arrays.copyOfRange(vectorParts, startIndex, endIndex));
            sendVectorToFollower(division, Integer.parseInt(followersIds[i]), requestId);
            startIndex = endIndex;
        }
    }

    private void sendVectorToFollower(String division, int id, String requestId) {
        strategy.sendMessage("VECTOR|" + division + "|" + leaderId + "|" + requestId, id);
        System.out.println("[LEADER] Enviando vetor para o seguidor ID " + id + ": " + division);
    }

    public void handleVectorAsFollower(String vector, int leaderId, String requestId) {
        List<Integer> processedVector = Arrays.stream(vector.split(","))
                .map(Integer::parseInt)
                .toList();
        int sum = processedVector.stream().mapToInt(Integer::intValue).sum();

        strategy.addNodeAddress(leaderId, new InetSocketAddress("localhost", 9000 + leaderId));

        String message = "RESULT|" + sum + "|" + requestId;
        strategy.addNodeAddress(leaderId, new InetSocketAddress("localhost", 9000 + leaderId));

        strategy.sendMessage(message, leaderId);
    }


    public void setFollowers(List<Integer> followers) {
        this.followers = followers;
    }

    public void addPartialResult(String requestId, int partial) {
        partialResults.computeIfAbsent(requestId, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(partial);

        System.out.println("[LEADER] Resultado parcial adicionado: " + partial + " | RequestID: " + requestId);

        List<Integer> parciais = partialResults.get(requestId);
        System.out.println("[LEADER] Parciais atuais para " + requestId + ": " + parciais);
    }

    public boolean allResultsReceived(String requestId) {
        List<Integer> partials = partialResults.get(requestId);
        return partials != null && partials.size() == followers.size();
    }

    public int getTotalResult(String requestId) {
        List<Integer> partials = partialResults.get(requestId);
        if (partials != null) {
            System.out.println("[LEADER] Parciais de " + requestId + ": " + partials);
            return partials.stream().mapToInt(Integer::intValue).sum();
        }
        return 0;
    }


    public void clearResults(String requestId) {
        partialResults.remove(requestId);
    }
    public void startResultTimeout(String requestId, CommunicationStrategy strategy) {
        scheduler.schedule(() -> {
            if (!allResultsReceived(requestId)) {
                int total = getTotalResult(requestId); // soma o que tem
                System.out.println("[LEADER] Timeout! Somando parciais para " + requestId + ": " + total);
                strategy.sendMessage("RESULT|" + total + "|" + requestId, 0);
                clearResults(requestId);
            }
        }, 10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Uso: java -cp bin server.Server <PROTOCOLO> <PORTA> <GATEWAY_HOST:PORTA>");
            return;
        }

        String protocolo = args[0];
        int port = Integer.parseInt(args[1]);
        String[] gatewayParts = args[2].split(":");
        String gatewayHost = gatewayParts[0];
        int gatewayPort = Integer.parseInt(gatewayParts[1]);

        ConcurrentHashMap<Integer, InetSocketAddress> serverAddresses = new ConcurrentHashMap<>();
        serverAddresses.put(0, new InetSocketAddress(gatewayHost, gatewayPort));

        CommunicationStrategy strategy = switch (protocolo.toUpperCase()) {
            case "TCP" -> new communication.TCPCommunicationStrategy(serverAddresses, port);
            case "UDP" -> new communication.UDPCommunicationStrategy(serverAddresses, port);
            case "HTTP" -> new communication.HTTPCommunicationStrategy(serverAddresses, port);
            default -> throw new IllegalArgumentException("Protocolo inválido: " + protocolo);
        };

        Server server = new Server(port, strategy);
        server.start();
    }
}
