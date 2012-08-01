package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.net.SocketException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17254;
    private static final StatsDClient client = new StatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT);
    private static final List<String> received = new ArrayList<String>();
    private static DatagramSocket server;

    @BeforeClass
    static public void start() throws Exception {
        server = new DatagramSocket(STATSD_SERVER_PORT);
        new Thread(new Runnable() {
            @Override public void run() {
                while (!server.isClosed()) {
                    try {
                        final DatagramPacket packet = new DatagramPacket(new byte[256], 256);
                        server.receive(packet);
                        String in = new String(packet.getData()).trim();
                        received.add(in);
                        System.out.println(in);
                    } catch (Exception e) { }
                }
            }
        }).start();
    }

    @AfterClass
    static public void stop() throws Exception {
        server.close();
        client.stop();
    }

    @After
    public void clearIncoming() throws Exception {
        received.clear();
    }

    @Test(timeout=5000L) public void
    increments_correctly() throws Exception {
        client.increment("blah");
        while (received.isEmpty()) { Thread.sleep(50L); }
        assertThat(received, contains("my.prefix.blah:1|c"));
    }

    @Test(timeout=5000L) public void
    increments_by_x_correctly() throws Exception {
        client.increment("blah", 123);
        while (received.isEmpty()) { Thread.sleep(50L); }
        assertThat(received, contains("my.prefix.blah:123|c"));
    }

    @Test(timeout=5000L) public void
    decrements_correctly() throws Exception {
        client.decrement("blah");
        while (received.isEmpty()) { Thread.sleep(50L); }
        assertThat(received, contains("my.prefix.blah:-1|c"));
    }

    @Test(timeout=5000L) public void
    decrements_by_x_correctly() throws Exception {
        client.decrement("blah", 123);
        while (received.isEmpty()) { Thread.sleep(50L); }
        assertThat(received, contains("my.prefix.blah:-123|c"));
    }
}
