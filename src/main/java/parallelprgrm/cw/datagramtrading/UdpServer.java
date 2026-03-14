package parallelprgrm.cw.datagramtrading;

import java.io.File;
import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UdpServer {
    private static final int PORT = 9876;
    private static final int BUFFER_SIZE = 16384; // 16 KB буфер

    private static final Map<String, Map<Integer, byte[]>> imageBuffers = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            System.out.println("UDP Сервер запущен на порту " + PORT);
            byte[] buffer = new byte[BUFFER_SIZE];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet); // Блокирующий вызов

                // Копируем только реально полученные байты
                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
                String header = new String(data, 0, Math.min(data.length, 50), StandardCharsets.UTF_8);

                String clientInfo = packet.getAddress().getHostAddress() + ":" + packet.getPort();

                if (header.startsWith("TXT|")) {
                    String message = new String(data, 4, data.length - 4, StandardCharsets.UTF_8);
                    System.out.println("Текст от " + clientInfo + ": " + message);
                    sendReply(socket, packet, "TXT|Сервер получил сообщение");
                }
                else if (header.startsWith("IMG|")) {
                    processImageChunk(data, clientInfo, socket, packet);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processImageChunk(byte[] data, String clientInfo, DatagramSocket socket, DatagramPacket packet) {
        try {
            // Ищем конец заголовка (первый символ | после метаданных)
            int headerEnd = -1;
            int pipeCount = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '|') {
                    pipeCount++;
                    if (pipeCount == 4) { // IMG|id|total|index|
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

            // Выделяем сами байты картинки
            byte[] chunkData = Arrays.copyOfRange(data, headerEnd + 1, data.length);

            // Сохраняем фрагмент
            imageBuffers.putIfAbsent(imageId, new HashMap<>());
            imageBuffers.get(imageId).put(chunkIndex, chunkData);

            System.out.println("Получен фрагмент " + chunkIndex + "/" + totalChunks + " картинки " + imageId);

            // Если собрали все фрагменты
            if (imageBuffers.get(imageId).size() == totalChunks) {
                saveImage(imageId, totalChunks);
                sendReply(socket, packet, "TXT|Сервер успешно принял и сохранил картинку!");
                imageBuffers.remove(imageId); // Очищаем память
            }
        } catch (Exception e) {
            System.err.println("Ошибка обработки фрагмента картинки: " + e.getMessage());
        }
    }

    private static void saveImage(String imageId, int totalChunks) {
        String directoryName = "saved";
        File directory = new File(directoryName);

        // Если папка "saved" не существует, создаем её
        if (!directory.exists()) {
            directory.mkdirs();
        }

        String filePath = directoryName + File.separator + "received_" + imageId + ".jpg";

        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            Map<Integer, byte[]> chunks = imageBuffers.get(imageId);
            for (int i = 0; i < totalChunks; i++) {
                fos.write(chunks.get(i));
            }
            System.out.println("Картинка " + imageId + " успешно собрана и сохранена в папку '" + directoryName + "'!");
        } catch (Exception e) {
            System.err.println("Ошибка при сохранении картинки: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void sendReply(DatagramSocket socket, DatagramPacket clientPacket, String reply) throws Exception {
        byte[] replyData = reply.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(replyData, replyData.length, clientPacket.getAddress(), clientPacket.getPort());
        socket.send(packet);
    }
}

