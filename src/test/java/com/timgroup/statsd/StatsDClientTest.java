package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.net.SocketException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Method;

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

    @Test public void
    always_include_sample_if_rate_is_1() throws Exception {
        assertThat(client.sample("foo", 1.0), equalTo("foo"));
    }

    @Test public void
    never_include_sample_if_rate_is_0() throws Exception {
        assertNull(client.sample("foo", 0.0));
    }

    @Test public void
    samples_at_correct_rate() throws Exception {
        int total = 10000;
        int sampled = 0;
        double rate = 0.1;
        StatsDClient.RNG.setSeed(1234567890l);
        for (int i = 0; i < total; i++) {
            String out = client.sample("foo", rate);
            if (out != null) { sampled++; }
        }
        // With the seed given about, this should be 1006, but we'll use a
        // delta just in case of platform differences.
        assertEquals(1000, sampled, total*0.01);
    }

    @Test public void
    appends_sample_rate_to_message() throws Exception {
        String out = null;
        double rate = 0.8;
        while (out == null) { out = client.sample("foo", rate); }
        assertThat(out, equalTo("foo|@0.8"));
    }
}
