package edu.umd.cs.hcil.twitter.streamer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

/**
 * Created by cbuntain on 7/19/15.
 */
public class SocketSender {

    private final ServerSocket mServer;
    private Socket mSocket;
    private PrintWriter mOutput;

    public SocketSender(String host, int port) throws IOException {
        mServer = new ServerSocket(port, 3, InetAddress.getAllByName(host)[0]);
    }

    public void accept() throws IOException {
        mSocket = mServer.accept();

        mOutput = new PrintWriter(mSocket.getOutputStream(), true);
    }

    public void send(List<String> messages) {
        for ( String s : messages ) {
            mOutput.println(s);
        }
    }

    public void send(String message) {
        mOutput.println(message);
    }

}
