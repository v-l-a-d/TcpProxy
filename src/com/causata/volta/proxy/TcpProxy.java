package com.causata.volta.proxy;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;

import java.net.*;
import java.util.EnumSet;
import java.util.Map;

/**
 */
public class TcpProxy {

    private final static Map<Integer, InetSocketAddress> ROUTES;

    static {
        try {
            ROUTES = ImmutableMap.of(60021, new InetSocketAddress(InetAddress.getLocalHost().getHostName(), 60020));
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to query local hostname");
        }
    }

    public static void main(String[] args) {
        SelectorThread selectorThread = null;
        Throttler throttle = new Throttler();

        try {
            selectorThread = new SelectorThread(ROUTES, throttle);

            for (Integer port : ROUTES.keySet()) {
                selectorThread.startServerSocket(new InetSocketAddress(port));
            }

            // Start a UDP server socket to receive commands
            String port = args.length > 0 ? args[0] : "23132";
            int cmdPort = Integer.valueOf(port);
            byte[] cmdBuffer = new byte[1024];
            DatagramSocket cmdSocket = new DatagramSocket(cmdPort);
            DatagramPacket packet = new DatagramPacket(cmdBuffer, cmdBuffer.length);

            cmdSocket.receive(packet);

            while (packet.getLength() > 0) {

                String cmd = new String(packet.getData(), 0, packet.getLength());

                try {
                    Throttler.Adjustment adjustment = Throttler.Adjustment.valueOf(cmd);
                    throttle.adjustThrottle(adjustment);
                } catch (IllegalArgumentException ix) {
                    System.out.println("You wot? " + cmd + "? " + EnumSet.allOf(Throttler.Adjustment.class));
                }

                cmdSocket.receive(packet);
            }

        } catch (Exception e) {
            System.out.println("ERROR: " + e);
            e.printStackTrace(System.out);
        } finally {
            Closeables.closeQuietly(selectorThread);
            Closeables.closeQuietly(throttle);
        }
    }
}
