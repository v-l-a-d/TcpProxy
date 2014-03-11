package com.causata.volta.proxy;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Map;
import java.util.Scanner;

/**
 */
public class TcpRelay {

    private final static Map<Integer, InetSocketAddress> ROUTES =
            ImmutableMap.of(60021, new InetSocketAddress("localhost.localdomain", 60020));

    public static void main(String[] args) {
        SelectorThread selectorThread = null;
        Throttler throttle = new Throttler();

        try {
            selectorThread = new SelectorThread(ROUTES, throttle);

            for (Integer port : ROUTES.keySet()) {
                selectorThread.startServerSocket(new InetSocketAddress(port));
            }

            Scanner scanner = new Scanner(System.in);
            String cmd = scanner.nextLine();

            while (cmd != null) {
                try {
                    Throttler.Adjustment adjustment = Throttler.Adjustment.valueOf(cmd);
                    throttle.adjustThrottle(adjustment);
                } catch (IllegalArgumentException ix) {
                    System.out.println("You wot? " + EnumSet.allOf(Throttler.Adjustment.class));
                }
                cmd = scanner.nextLine();
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
