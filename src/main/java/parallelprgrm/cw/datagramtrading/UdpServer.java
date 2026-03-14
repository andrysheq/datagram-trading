package parallelprgrm.cw.datagramtrading;

import parallelprgrm.cw.datagramtrading.model.Client;

import java.io.File;
import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class UdpServer {
    private static final int PORT = 9876;
    private static final int BUFFER_SIZE = 16384;

    private static final Map<String, Map<Integer, byte[]>> imageBuffers = new ConcurrentHashMap<>();

    private static final Set<String> activeClients = ConcurrentHashMap.newKeySet();
    private static final Map<String, Client> clientNodes = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            System.out.println("UDP Чат-сервер запущен на порту " + PORT);
            byte[] buffer = new byte[BUFFER_SIZE];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                registerClientIfNotExists(packet);

                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
                String header = new String(data, 0, Math.min(data.length, 50), StandardCharsets.UTF_8);

                String clientInfo = packet.getAddress().getHostAddress() + ":" + packet.getPort();

                if (header.startsWith("HI|")) {
                    System.out.println("Клиент " + clientInfo + " успешно зарегистрирован.");
                    String welcomeMsg = "TXT|Система: Клиент [" + packet.getPort() + "] присоединился к чату!";
                    broadcastMessage(socket, welcomeMsg, packet.getPort());
                }

                else if (header.startsWith("TXT|")) {
                    String message = new String(data, 4, data.length - 4, StandardCharsets.UTF_8);
                    System.out.println("Текст от " + clientInfo + ": " + message);

                    String broadcastMsg = "TXT|Клиент [" + packet.getPort() + "]: " + message;
                    broadcastMessage(socket, broadcastMsg, packet.getPort());
                }

                else if (header.startsWith("IMG|")) {
                    processImageChunk(data, clientInfo, socket, packet);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void registerClientIfNotExists(DatagramPacket packet) {
        String key = packet.getAddress().getHostAddress() + ":" + packet.getPort();
        if (activeClients.add(key)) {
            clientNodes.put(key, new Client(packet.getAddress(), packet.getPort()));
            System.out.println("Новый клиент подключился: " + key);
        }
    }

    private static void broadcastMessage(DatagramSocket socket, String msg, int senderPort) {
        byte[] replyData = msg.getBytes(StandardCharsets.UTF_8);

        for (Client node : clientNodes.values()) {
            if (node.getPort() != senderPort) {
                try {
                    DatagramPacket packet = new DatagramPacket(replyData, replyData.length, node.getIp(), node.getPort());
                    socket.send(packet);
                } catch (Exception e) {
                    System.err.println("Ошибка рассылки клиенту: " + e.getMessage());
                }
            }
        }
    }

    private static void processImageChunk(byte[] data, String clientInfo, DatagramSocket socket, DatagramPacket packet) {
        try {
            int headerEnd = -1;
            int pipeCount = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '|') {
                    pipeCount++;
                    // Было 4, теперь ищем 4, так как клиент присылает IMG|id|total|index|
                    if (pipeCount == 4) {
                        headerEnd = i;
                        break;
                    }
                }
            }
            if (headerEnd == -1) return;

            String headerStr = new String(data, 0, headerEnd, StandardCharsets.UTF_8);
            String[] parts = headerStr.split("\\|");

            String imageId = parts[1];
            int totalChunks = Integer.parseInt(parts[2]);
            int chunkIndex = Integer.parseInt(parts[3]);
            int senderPort = packet.getPort(); // Получаем порт отправителя

            byte[] chunkData = Arrays.copyOfRange(data, headerEnd + 1, data.length);

            imageBuffers.putIfAbsent(imageId, new HashMap<>());
            imageBuffers.get(imageId).put(chunkIndex, chunkData);

            String newHeader = headerStr + "|" + senderPort + "|";
            byte[] newHeaderBytes = newHeader.getBytes(StandardCharsets.UTF_8);

            byte[] broadcastData = new byte[newHeaderBytes.length + chunkData.length];
            System.arraycopy(newHeaderBytes, 0, broadcastData, 0, newHeaderBytes.length);
            System.arraycopy(chunkData, 0, broadcastData, newHeaderBytes.length, chunkData.length);

            broadcastImageChunk(socket, broadcastData, senderPort);

            if (imageBuffers.get(imageId).size() == totalChunks) {
                saveImage(imageId, totalChunks);
                imageBuffers.remove(imageId);
            }
        } catch (Exception e) {
            System.err.println("Ошибка обработки фрагмента: " + e.getMessage());
        }
    }

    private static void broadcastImageChunk(DatagramSocket socket, byte[] data, int senderPort) {
        for (Client node : clientNodes.values()) {
            if (node.getPort() != senderPort) {
                try {
                    DatagramPacket packet = new DatagramPacket(data, data.length, node.getIp(), node.getPort());
                    socket.send(packet);
                } catch (Exception e) {
                    System.err.println("Ошибка рассылки картинки: " + e.getMessage());
                }
            }
        }
    }

    private static void saveImage(String imageId, int totalChunks) {
        String directoryName = "saved";
        File directory = new File(directoryName);
        if (!directory.exists()) directory.mkdirs();

        File imageFile = new File(directoryName + File.separator + "received_" + imageId + ".jpg");

        try (FileOutputStream fos = new FileOutputStream(imageFile)) {
            Map<Integer, byte[]> chunks = imageBuffers.get(imageId);
            for (int i = 0; i < totalChunks; i++) fos.write(chunks.get(i));

            System.out.println("Картинка успешно сохранена на сервере!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
