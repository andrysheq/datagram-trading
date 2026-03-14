package parallelprgrm.cw.datagramtrading;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.FileInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class UdpClientFx extends Application {
    private DatagramSocket socket;
    private InetAddress serverAddress;
    private static final int PORT = 9876;
    private static final int CHUNK_SIZE = 8192;

    private VBox chatBox;
    private ScrollPane scrollPane;
    private TextField inputField;
    private final Map<String, Map<Integer, byte[]>> receiveImageBuffers = new ConcurrentHashMap<>();

    @Override
    public void start(Stage primaryStage) {

        chatBox = new VBox(15);
        chatBox.setPadding(new Insets(10));
        chatBox.setStyle("-fx-background-color: #F5F5F5;");

        scrollPane = new ScrollPane(chatBox);
        scrollPane.setFitToWidth(true);
        scrollPane.setStyle("-fx-background: #F5F5F5; -fx-border-color: transparent;");

        chatBox.heightProperty().addListener((observable, oldValue, newValue) -> scrollPane.setVvalue(1.0));

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

        VBox root = new VBox(scrollPane, bottomPanel);
        VBox.setVgrow(scrollPane, Priority.ALWAYS);
        root.setPadding(new Insets(15));
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 500, 600);
        primaryStage.setTitle("UDP Мессенджер");
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> {
            if (socket != null && !socket.isClosed()) socket.close();
        });
        primaryStage.show();

        initNetwork();
    }

    private void initNetwork() {
        try {
            socket = new DatagramSocket();
            serverAddress = InetAddress.getByName("127.0.0.1");

            Thread listenerThread = new Thread(this::listenForReplies);
            listenerThread.setDaemon(true);
            listenerThread.start();

            addTextMessage("Система", "Готов к работе. Отправьте первое сообщение, чтобы получать ответы от сервера.", "#888888");
        } catch (Exception e) {
            addTextMessage("Система", "Ошибка сети: " + e.getMessage(), "red");
        }
    }

    private void sendText() {
        String msg = inputField.getText().trim();
        if (msg.isEmpty()) return;

        try {
            String payload = "TXT|" + msg;
            byte[] data = payload.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(data, data.length, serverAddress, PORT));

            addTextMessage("Вы", msg, "#0084FF");
            inputField.clear();
        } catch (Exception e) {
            addTextMessage("Ошибка", e.getMessage(), "red");
        }
    }

    private void sendImage(Stage stage) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Изображения", "*.jpg", "*.png", "*.jpeg"));
        File file = fileChooser.showOpenDialog(stage);

        if (file != null) {
            addImageMessage("Вы", file);
            new Thread(() -> processAndSendFile(file)).start();
        }
    }

    private void processAndSendFile(File file) {
        String imageId = UUID.randomUUID().toString().substring(0, 8);
        try (FileInputStream fis = new FileInputStream(file)) {
            long fileSize = file.length();
            int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;
            int chunkIndex = 0;

            while ((bytesRead = fis.read(buffer)) != -1) {
                String header = "IMG|" + imageId + "|" + totalChunks + "|" + chunkIndex + "|";
                byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);

                byte[] packetData = new byte[headerBytes.length + bytesRead];
                System.arraycopy(headerBytes, 0, packetData, 0, headerBytes.length);
                System.arraycopy(buffer, 0, packetData, headerBytes.length, bytesRead);

                socket.send(new DatagramPacket(packetData, packetData.length, serverAddress, PORT));
                chunkIndex++;
                Thread.sleep(5);
            }
        } catch (Exception e) {
            addTextMessage("Система", "Ошибка отправки файла: " + e.getMessage(), "red");
        }
    }

    private void listenForReplies() {
        try {
            byte[] buffer = new byte[CHUNK_SIZE + 256];
            while (!socket.isClosed()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
                String header = new String(data, 0, Math.min(data.length, 50), StandardCharsets.UTF_8);

                if (header.startsWith("TXT|")) {
                    String cleanMsg = new String(data, 4, data.length - 4, StandardCharsets.UTF_8);
                    addTextMessage("Чат", cleanMsg, "#28a745");
                }
                else if (header.startsWith("IMG|")) {
                    processIncomingImageChunk(data);
                }
            }
        } catch (Exception e) {
            if (!socket.isClosed()) {
                addTextMessage("Система", "Соединение разорвано.", "red");
            }
        }
    }

    private void processIncomingImageChunk(byte[] data) {
        try {
            int headerEnd = -1;
            int pipeCount = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '|') {
                    pipeCount++;
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

            byte[] chunkData = Arrays.copyOfRange(data, headerEnd + 1, data.length);

            receiveImageBuffers.putIfAbsent(imageId, new HashMap<>());
            receiveImageBuffers.get(imageId).put(chunkIndex, chunkData);

            if (receiveImageBuffers.get(imageId).size() == totalChunks) {

                File tempFile = File.createTempFile("chat_img_" + imageId, ".jpg");
                tempFile.deleteOnExit();

                try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile)) {
                    Map<Integer, byte[]> chunks = receiveImageBuffers.get(imageId);
                    for (int i = 0; i < totalChunks; i++) {
                        fos.write(chunks.get(i));
                    }
                }

                addImageMessage("Собеседник", tempFile);

                // Очищаем память
                receiveImageBuffers.remove(imageId);
            }
        } catch (Exception e) {
            System.err.println("Ошибка сборки картинки на клиенте: " + e.getMessage());
        }
    }


    private void addTextMessage(String sender, String text, String color) {
        Platform.runLater(() -> {
            Label label = new Label(sender + ": " + text);
            label.setWrapText(true);
            label.setStyle("-fx-font-family: 'Segoe UI'; -fx-font-size: 14px; -fx-text-fill: " + color + ";");
            chatBox.getChildren().add(label);
        });
    }

    private void addImageMessage(String sender, File file) {
        Platform.runLater(() -> {
            Label headerLabel = new Label(sender + " отправил(а) изображение:");
            headerLabel.setStyle("-fx-font-family: 'Segoe UI'; -fx-font-size: 14px; -fx-font-weight: bold; -fx-text-fill: #0084FF;");

            Image img = new Image(file.toURI().toString());
            ImageView imageView = new ImageView(img);

            imageView.setFitWidth(250);
            imageView.setPreserveRatio(true);

            VBox imageContainer = new VBox(5, headerLabel, imageView);
            imageContainer.setStyle("-fx-background-color: #E3F2FD; -fx-padding: 10px; -fx-background-radius: 10px;");

            chatBox.getChildren().add(imageContainer);
        });
    }

    public static void main(String[] args) {
        launch(args);
    }
}
