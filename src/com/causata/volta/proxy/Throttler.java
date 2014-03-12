package com.causata.volta.proxy;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks message rates and applies throttling.
 */
class Throttler implements Runnable, Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(Throttler.class);

    // TODO arbitrary bounds
    private final static int MIN_RATE = 1;
    private final static int MAX_RATE = 1000000;

    enum Adjustment {
        HARD,
        MEDIUM,
        SOFT,
        EASE
    }

    private final Semaphore permits;
    private final AtomicLong packets;

    private volatile boolean running;
    private volatile int rate;
    private volatile int increment;
    private volatile long lastAdjustmentTimeStamp;

    Throttler() {
        packets = new AtomicLong(0);

        // Initial rate settings
        rate = 10;
        increment = 1;
        permits = new Semaphore(rate);

        running = true;

        Thread t = new Thread(this, "Proxy permit dispenser");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Acquire a message transfer permit.
     * @throws InterruptedException
     */
    void acquire() throws InterruptedException {
        permits.acquire();
        packets.incrementAndGet();
    }

    /**
     * Release a message x-fer permit
     */
    void release() {
        permits.release();
    }

    void adjustThrottle(Adjustment adjustment) {
        // Set a new rate and increment
        int currentRate = rate;
        switch (adjustment) {
            case HARD:
                rate = Math.max(MIN_RATE, (int)(currentRate * 0.5));
                break;

            case MEDIUM:
                rate = Math.max(MIN_RATE, (int)(currentRate * 0.2));
                break;

            case SOFT:
                rate = Math.max(MIN_RATE, (int)(currentRate * 0.08));
                break;

            case EASE:
                rate = Math.min(MAX_RATE, currentRate + increment);
                break;
        }

        increment = Math.max(1, rate / 50);
        lastAdjustmentTimeStamp = System.nanoTime();
    }

    @Override
    public void close() throws IOException {
        running = false;
    }

    @Override
    public void run() {
        long nextTick = System.nanoTime();
        while (running) {
            try {
                // Check for further throttle easing due to idleness
                if (nextTick > lastAdjustmentTimeStamp + (5*60*1000*1000*1000L)) {
                    // Five minutes since any update - maybe ease.
                    if (permits.availablePermits() < rate) {
                        // Current rate is approaching or at the rate limit
                        adjustThrottle(Adjustment.EASE);
                    }
                }

                // Next tick in 20ms
                nextTick += (20L*1000*1000);

                // Top up the available permits to the desired rate - the increment is 1/50 of the per second rate.
                int available = permits.availablePermits();
                if (available < rate) {
                    permits.release(Math.min(increment, rate - available));
                }

                long sleepNanos = nextTick - System.nanoTime();
                if (sleepNanos > 1L*1000*1000) {
                    // Don't bother sleeping for less than 1ms
                    Uninterruptibles.sleepUninterruptibly(sleepNanos, TimeUnit.NANOSECONDS);
                }
            } catch (Throwable t) {
                LOG.error("Error dispensing put tokens!", t);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("Throttler: rate=%d, available permits=%d, packets transferred=%d", rate, permits.availablePermits(), packets.get());
    }
}
