package communication;

import connection.ClientConnection;
import connection.UDPClientConnection;
import message.MessageHandler;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.*;

public class UDPCommunicationStrategy implements CommunicationStrategy {

    private final ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses;
    private final int ownPort;
    private DatagramSocket socket;
    private MessageHandler messageHandler;
    private final ExecutorService executorService;

    public UDPCommunicationStrategy(ConcurrentHashMap<Integer, InetSocketAddress> nodeAddresses, int port) {
        this.nodeAddresses = nodeAddresses;
        this.ownPort = port;
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void startListening(int ownPort, MessageHandler messageHandler) {
        this.messageHandler = messageHandler;

        executorService.submit(() -> {
            try {
                socket = new DatagramSocket(ownPort);
                System.out.println("Servidor UDP iniciado na porta " + ownPort);

                while (!Thread.currentThread().isInterrupted()) {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    byte[] data = new byte[packet.getLength()];
                    System.arraycopy(packet.getData(), 0, data, 0, packet.getLength());

                    executorService.submit(() -> {
                        try {
                            String received = new String(data);
                            System.out.println("[UDP] Mensagem recebida de " + packet.getSocketAddress() + ": " + received);
                            messageHandler.handleMessage(received, new UDPClientConnection(socket, packet.getSocketAddress()));
                        } catch (Exception e) {
                            System.err.println("[ERRO] Exceção no handler UDP:");
                            e.printStackTrace();
                        }
                    });
                }

            } catch (IOException e) {
                System.err.println("[UDP] Erro ao iniciar servidor: " + e.getMessage());
            } finally {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    System.out.println("[UDP] Socket fechado.");
                }
            }
        });
    }

    @Override
    public void sendMessage(String messageStr, int targetId) {
        executorService.submit(() -> {
            try {
                InetSocketAddress address = nodeAddresses.get(targetId);
                if (address == null) {
                    System.err.println("[UDP] Endereço não encontrado para o ID " + targetId);
                    return;
                }

                byte[] data = messageStr.getBytes();
                DatagramPacket packet = new DatagramPacket(data, data.length, address.getAddress(), address.getPort());

                socket.send(packet);
                System.out.println("[UDP] Enviada mensagem para " + address + ": " + messageStr);
            } catch (IOException e) {
                System.err.println("[UDP] Erro ao enviar mensagem: " + e.getMessage());
            }
        });
    }

    @Override
    public void addNodeAddress(int id, InetSocketAddress address) {
        nodeAddresses.put(id, address);
    }

}
