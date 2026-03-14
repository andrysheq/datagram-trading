package parallelprgrm.cw.datagramtrading;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class UdpClientFx extends Application {
    private DatagramSocket socket;
    private InetAddress serverAddress;
    private static final int PORT = 9876;
    private static final int CHUNK_SIZE = 8192; // 8 KB полезной нагрузки на пакет

    private TextArea chatArea;
    private TextField inputField;

    @Override
    public void start(Stage primaryStage) {
        chatArea = new TextArea();
        chatArea.setEditable(false);
        chatArea.setWrapText(true);
        chatArea.setStyle("-fx-font-family: 'Segoe UI'; -fx-font-size: 14px; -fx-control-inner-background: #F5F5F5;");

        inputField = new TextField();
        inputField.setPromptText("Введите сообщение...");
        inputField.setStyle("-fx-font-family: 'Segoe UI'; -fx-font-size: 14px; -fx-padding: 10px; -fx-background-radius: 20px;");
        HBox.setHgrow(inputField, Priority.ALWAYS);

        Button sendBtn = new Button("Отправить");
        sendBtn.setStyle("-fx-background-color: #0084FF; -fx-text-fill: white; -fx-font-weight: bold; -fx-background-radius: 20px;");
        sendBtn.setOnAction(e -> sendText());
        inputField.setOnAction(e -> sendText());

        Button attachBtn = new Button("📎 Файл");
        attachBtn.setStyle("-fx-background-color: #E4E6EB; -fx-font-weight: bold; -fx-background-radius: 20px;");
        attachBtn.setOnAction(e -> sendImage(primaryStage));

        HBox bottomPanel = new HBox(10, attachBtn, inputField, sendBtn);
        bottomPanel.setPadding(new Insets(10, 0, 0, 0));

        VBox root = new VBox(chatArea, bottomPanel);
        VBox.setVgrow(chatArea, Priority.ALWAYS);
        root.setPadding(new Insets(15));
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 500, 400);
        primaryStage.setTitle("UDP Мессенджер");
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> {
            if (socket != null && !socket.isClosed()) socket.close();
        });
        primaryStage.show();

        try {
            socket = new DatagramSocket();
            serverAddress = InetAddress.getByName("127.0.0.1");

            Thread listenerThread = new Thread(this::listenForReplies);
            listenerThread.setDaemon(true);
            listenerThread.start();

            log("Подключено к серверу UDP.");
        } catch (Exception e) {
            log("Ошибка сети: " + e.getMessage());
        }
    }

    private void sendText() {
        String msg = inputField.getText().trim();
        if (msg.isEmpty()) return;

        try {
            String payload = "TXT|" + msg;
            byte[] data = payload.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(data, data.length, serverAddress, PORT));
            log("Вы: " + msg);
            inputField.clear();
        } catch (Exception e) {
            log("Ошибка отправки текста: " + e.getMessage());
        }
    }

    private void sendImage(Stage stage) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Изображения", "*.jpg", "*.png", "*.jpeg"));
        File file = fileChooser.showOpenDialog(stage);

        if (file != null) {
            new Thread(() -> processAndSendFile(file)).start();
        }
    }

    private void processAndSendFile(File file) {
        String imageId = UUID.randomUUID().toString().substring(0, 8); // Уникальный ID для картинки

        try (FileInputStream fis = new FileInputStream(file)) {
            long fileSize = file.length();
            int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

            Platform.runLater(() -> log("Отправка картинки " + file.getName() + " (" + totalChunks + " пакетов)..."));

            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;
            int chunkIndex = 0;

            while ((bytesRead = fis.read(buffer)) != -1) {
                // Формируем заголовок: IMG|id|total|index|
                String header = "IMG|" + imageId + "|" + totalChunks + "|" + chunkIndex + "|";
                byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);

                // Объединяем заголовок и реальные байты картинки
                byte[] packetData = new byte[headerBytes.length + bytesRead];
                System.arraycopy(headerBytes, 0, packetData, 0, headerBytes.length);
                System.arraycopy(buffer, 0, packetData, headerBytes.length, bytesRead);

                // Отправляем чанк
                DatagramPacket packet = new DatagramPacket(packetData, packetData.length, serverAddress, PORT);
                socket.send(packet);

                chunkIndex++;

                // Небольшая пауза, чтобы не перегрузить UDP буфер операционной системы и избежать потери пакетов
                Thread.sleep(5);
            }
            Platform.runLater(() -> log("Картинка полностью отправлена в сеть."));

        } catch (Exception e) {
            Platform.runLater(() -> log("Ошибка при отправке файла: " + e.getMessage()));
        }
    }

    private void listenForReplies() {
        try {
            byte[] buffer = new byte[1024];
            while (!socket.isClosed()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String reply = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                if (reply.startsWith("TXT|")) {
                    String cleanMsg = reply.substring(4);
                    Platform.runLater(() -> log("Сервер: " + cleanMsg));
                }
            }
        } catch (Exception e) {
            if (!socket.isClosed()) {
                Platform.runLater(() -> log("Соединение разорвано."));
            }
        }
    }

    private void log(String message) {
        chatArea.appendText(message + "\n");
    }

    public static void main(String[] args) {
        launch(args);
    }
}
