package gateway;

import communication.CommunicationStrategy;
import message.GatewayMessageHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ApiGateway {
    private final CommunicationStrategy communicationStrategy;
    private final int port;
    private final Map<Integer, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private final long heartbeatTimeout = 10000;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ConcurrentHashMap<Integer, InetSocketAddress> serverAddresses;
    private int leaderId = -1;
    private InetSocketAddress repositoryAddress;

    public ApiGateway(CommunicationStrategy communicationStrategy, int port, ConcurrentHashMap<Integer, InetSocketAddress> serverAddresses) {
        this.communicationStrategy = communicationStrategy;
        this.port = port;
        this.executorService = Executors.newCachedThreadPool();
        this.serverAddresses = serverAddresses;
        System.out.println("[API-GATEWAY] Iniciada na porta " + port);
    }

    public void start() {
        executorService.submit(() -> communicationStrategy.startListening(port, new GatewayMessageHandler(this, communicationStrategy)));

        executorService.submit(() -> {
            try {
                Thread.sleep(20000);
                runElection();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        executorService.submit(this::startHeartbeatMonitor);

        scheduler.scheduleAtFixedRate(() -> {
            int currentLeaderId = getLeaderId();
            if (currentLeaderId == -1) return;

            List<Integer> followersList = serverAddresses.keySet().stream()
                    .filter(id -> id != currentLeaderId && id < 100)
                    .toList();

            String followersString = followersList.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            String message = "SET_FOLLOWERS|" + followersString;
            communicationStrategy.sendMessage(message, currentLeaderId);
            System.out.println("[GATEWAY] SET_FOLLOWERS enviado para líder " + currentLeaderId + ": " + followersString);

        }, 0, 10, TimeUnit.SECONDS);
    }

    private void startHeartbeatMonitor() {
        while (true) {
            try {
                Thread.sleep(heartbeatTimeout);
                long now = System.currentTimeMillis();
                for (Integer serverId : new ArrayList<>(lastHeartbeat.keySet())) {
                    long last = lastHeartbeat.get(serverId);
                    if (now - last > heartbeatTimeout) {
                        handleHeartbeatTimeout(serverId);
                        lastHeartbeat.remove(serverId);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void runElection() {
        executorService.submit(() -> {
            if (serverAddresses.isEmpty()) {
                System.out.println("[GATEWAY] Nenhum servidor ativo para eleição.");
                return;
            }

            int newLeaderId = serverAddresses.keySet().stream()
                    .filter(id -> id < 100)
                    .max(Integer::compare)
                    .orElse(-1);

            leaderId = newLeaderId;
            System.out.println("[GATEWAY] Eleição concluída. Líder eleito: ID=" + leaderId);

            for (Integer serverId : serverAddresses.keySet()) {
                if (serverId == (repositoryAddress != null ? repositoryAddress.getPort() - 9000 : -2)) continue;

                String message = "LEADER|" + leaderId;
                System.out.println("[GATEWAY] Enviando mensagem LEADER para servidor ID: " + serverId);
                communicationStrategy.sendMessage(message, serverId);
            }

            List<Integer> followersList = serverAddresses.keySet().stream()
                    .filter(id -> id != leaderId && id < 100)
                    .toList();

            if (!followersList.isEmpty()) {
                String followerIds = followersList.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));

                String msg = "SET_FOLLOWERS|" + followerIds;
                System.out.println("[GATEWAY] Enviando seguidores para o líder: " + msg);
                communicationStrategy.sendMessage(msg, leaderId);
            } else {
                System.out.println("[GATEWAY] Nenhum follower disponível.");
            }
        });
    }

    public void handleHeartbeat(int serverId) {
        executorService.submit(() -> {
            lastHeartbeat.put(serverId, System.currentTimeMillis());
            if (isDataStore(serverId)) {
                if (repositoryAddress == null) {
                    int port = serverId + 9000;
                    repositoryAddress = new InetSocketAddress("localhost", port);
                    System.out.println("[GATEWAY] Registrando DataStore: ID=" + serverId + ", porta=" + port);
                    serverAddresses.putIfAbsent(serverId, repositoryAddress);
                }
            } else {
                boolean added = serverAddresses.putIfAbsent(serverId, new InetSocketAddress("localhost", serverId + 9000)) == null;
                if (added) {
                    System.out.println("[GATEWAY] Adicionando novo servidor: ID=" + serverId + ", porta=" + (serverId + 9000));
                    runElection();
                }
            }
        });
    }

    private boolean isDataStore(int id) {
        return id == 100;
    }

    private void handleHeartbeatTimeout(int serverId) {
        System.out.println("[GATEWAY] Falha detectada no servidor ID: " + serverId);
        serverAddresses.remove(serverId);

        if (serverId == leaderId) {
            System.out.println("[GATEWAY] O líder caiu. Reiniciando eleição...");
            leaderId = -1;
            runElection();
        } else {
            System.out.println("[GATEWAY] Follower caiu. Notificando o líder e reiniciando eleição...");
            if (leaderId != -1 && serverAddresses.containsKey(leaderId)) {
                communicationStrategy.sendMessage("FOLLOWER_FAILED|" + serverId, leaderId);
            }
            runElection();
        }
    }

    public void handleVector(String vector, String requestId) {
        int currentLeaderId = getLeaderId();
        if (currentLeaderId == -1) {
            System.out.println("[GATEWAY] Nenhum líder disponível para receber o vetor.");
            return;
        }
        if (!serverAddresses.containsKey(currentLeaderId)) {
            System.out.println("[GATEWAY] Endereço do líder não encontrado.");
            return;
        }

        List<Integer> followersList = serverAddresses.keySet().stream()
                .filter(id -> id != currentLeaderId && id < 100)
                .toList();
        String followers = followersList.stream().map(String::valueOf).collect(Collectors.joining(","));
        String message = "VECTOR|" + vector + "|" + followers + "|" + requestId;
        communicationStrategy.sendMessage(message, currentLeaderId);
        System.out.println("[GATEWAY] Enviando vetor para líder.");
    }

    public int getLeaderId() {
        return leaderId;
    }

    public InetSocketAddress getRepositoryAddress() {
        return repositoryAddress;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Uso: java -cp bin gateway.ApiGateway <PROTOCOLO> <PORTA>");
            return;
        }

        String protocolo = args[0];
        int port = Integer.parseInt(args[1]);

        ConcurrentHashMap<Integer, InetSocketAddress> serverAddresses = new ConcurrentHashMap<>();

        CommunicationStrategy strategy = switch (protocolo.toUpperCase()) {
            case "TCP" -> new communication.TCPCommunicationStrategy(serverAddresses, port);
            case "UDP" -> new communication.UDPCommunicationStrategy(serverAddresses, port);
            case "HTTP" -> new communication.HTTPCommunicationStrategy(serverAddresses, port);
            default -> throw new IllegalArgumentException("Protocolo inválido: " + protocolo);
        };

        ApiGateway gateway = new ApiGateway(strategy, port, serverAddresses);
        gateway.start();
    }


}
