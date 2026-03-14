package parallelprgrm.cw.datagramtrading.model;

import java.net.InetAddress;

public class Client {

    private InetAddress ip;
    private int port;

    public Client(InetAddress ip, int port) { this.ip = ip; this.port = port; }

    public InetAddress getIp() { return ip; }
    public int getPort() { return port; }
}