package com.timgroup.statsd;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A simple StatsD client implementation facilitating metrics recording.
 * 
 * <p>Upon instantiation, this client will establish a socket connection to a StatsD instance
 * running on the specified host and port. Metrics are then sent over this connection as they are
 * received by the client.
 * </p>
 * 
 * <p>Three key methods are provided for the submission of data-points for the application under
 * scrutiny:
 * <ul>
 *   <li>{@link #incrementCounter} - adds one to the value of the specified named counter</li>
 *   <li>{@link #recordGaugeValue} - records the latest fixed value for the specified named gauge</li>
 *   <li>{@link #recordExecutionTime} - records an execution time in milliseconds for the specified named operation</li>
 * </ul>
 * From the perspective of the application, these methods are non-blocking, with the resulting
 * IO operations being carried out in a separate thread. Furthermore, these methods are guaranteed
 * not to throw an exception which may disrupt application execution.
 * </p>
 * 
 * <p>As part of a clean system shutdown, the {@link #stop()} method should be invoked
 * on any StatsD clients.</p>
 * 
 * @author Tom Denley
 *
 */
public final class StatsDClient {

    private final String prefix;
    private final DatagramSocket clientSocket;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();
        @Override public Thread newThread(Runnable r) {
            Thread result = delegate.newThread(r);
            result.setName("StatsD-" + result.getName());
            return result;
        }
    });

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established.
     * 
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     */
    public StatsDClient(String prefix, String hostname, int port) {
        this.prefix = prefix;
        
        try {
            this.clientSocket = new DatagramSocket();
            this.clientSocket.connect(new InetSocketAddress(hostname, port));
        } catch (SocketException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if
     * the socket cannot be closed.
     */
    public void stop() {
        try {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }

    /**
     * Increments the specified counter by one.
     * 
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     * 
     * @param aspect
     *     the name of the counter to increment
     */
    public void increment(String aspect) {
        send(String.format("%s.%s:%d|c", prefix, aspect, 1));
    }

    /**
     * Records the latest fixed value for the specified named gauge.
     * 
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     * 
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     */
    public void gauge(String aspect, int value) {
        send(String.format("%s.%s:%d|g", prefix, aspect, value));
    }

    /**
     * Records an execution time in milliseconds for the specified named operation.
     * 
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     * 
     * @param aspect
     *     the name of the timed operation
     * @param timeInMs
     *     the time in milliseconds
     */
    public void timing(String aspect, int timeInMs) {
        send(String.format("%s.%s:%d|ms", prefix, aspect, timeInMs));
    }

    private void send(final String message) {
        try {
            executor.execute(new Runnable() {
                @Override public void run() {
                    blockingSend(message);
                }
            });
        }
        catch (Exception e) {
            // we cannot allow exceptions to interfere with our caller's execution
        }
    }

    private void blockingSend(String message) {
        try {
            final byte[] sendData = message.getBytes();
            final DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length);
            clientSocket.send(sendPacket);
        } catch (Exception e) {
        }
    }
}
