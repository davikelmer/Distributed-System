package repository;

import communication.CommunicationStrategy;
import communication.HTTPCommunicationStrategy;
import communication.TCPCommunicationStrategy;
import communication.UDPCommunicationStrategy;
import connection.ClientConnection;
import message.MessageHandler;
import message.ServerMessageHandler;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ResultRepository implements MessageHandler {

    private final CommunicationStrategy strategy;
    private final Map<String, Integer> results = new ConcurrentHashMap<>();
    private int port;
    private final ExecutorService executorService;


    public ResultRepository(CommunicationStrategy strategy, int port) {
        this.port = port;
        this.strategy = strategy;
        this.executorService = Executors.newCachedThreadPool();
    }

    public void start() {
        executorService.submit(() ->
                strategy.startListening(port, this));
        startSendingHeartbeats();
        System.out.println("[REPOSITORY] ResultRepository iniciado na porta " + port);
    }

    private void startSendingHeartbeats() {
        int id = 100;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                String heartbeatMessage = "HEARTBEAT|" + id;
                strategy.sendMessage(heartbeatMessage, 0);
                System.out.println("[REPOSITORY] Heartbeat enviado para o gateway");
            }
        }, 0, 5000);
    }

    @Override
    public void handleMessage(String message, ClientConnection connection) {
        System.out.println("[REPOSITORY] Mensagem recebida: " + message);
        try {
            String[] parts = message.split("\\|");
            if (parts.length != 3 || !parts[0].equals("SAVE")) {
                System.err.println("[REPOSITORY] Mensagem inválida: " + message);
                return;
            }

            String result = parts[1];
            String requestId = parts[2];

            int intResult = Integer.parseInt(result);
            results.put(requestId, intResult);
            System.out.println("[REPOSITORY] Resultado armazenado: requestId=" + requestId + ", result=" + intResult);
        } catch (Exception e) {
            System.err.println("[REPOSITORY] Erro ao processar mensagem: " + message);
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Uso: java -cp bin datastore.ResultRepository <PROTOCOLO> <PORTA> <GATEWAY>");
            return;
        }

        String protocolo = args[0];
        int port = Integer.parseInt(args[1]);
        String[] gatewayParts = args[2].split(":");
        String gatewayHost = gatewayParts[0];
        int gatewayPort = Integer.parseInt(gatewayParts[1]);

        ConcurrentHashMap<Integer, InetSocketAddress> serverAddresses = new ConcurrentHashMap<>();
        serverAddresses.putIfAbsent(0, new InetSocketAddress(gatewayHost, gatewayPort));

        CommunicationStrategy strategy = switch (protocolo.toUpperCase()) {
            case "TCP" -> new TCPCommunicationStrategy(serverAddresses, port);
            case "UDP" -> new UDPCommunicationStrategy(serverAddresses, port);
            case "HTTP" -> new HTTPCommunicationStrategy(serverAddresses, port);
            default -> throw new IllegalArgumentException("Protocolo inválido: " + protocolo);
        };

        ResultRepository resultRepository = new ResultRepository(strategy,port);
        resultRepository.start();
    }
}
