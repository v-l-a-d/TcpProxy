package com.causata.volta.proxy;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tracks message rates and applies throttling.
 */
class Throttler implements Runnable, Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(Throttler.class);

    // TODO arbitrary bounds
    private final static int MIN_RATE = 100;
    private final static int MAX_RATE = 1000000;

    enum Adjustment {
        HARD,
        MEDIUM,
        SOFT,
        EASE
    }

    private final Semaphore permits;
    private final int[] perSecondMessageRates;
    private int currentStamp;

    private volatile boolean running;
    private volatile int rate;
    private volatile int increment;
    private volatile long lastAdjustmentTimeStamp;

    private volatile int currentMessageRate;
    private volatile int packetcount;

    Throttler() {
        perSecondMessageRates = new int[60];

        // Initial rate settings
        rate = 1000;
        increment = rate / 50;
        permits = new Semaphore(rate);

        running = true;

        Thread t = new Thread(this, "Proxy permit dispenser");
        t.setDaemon(true);
        t.start();
    }

    private void incrementRate() {
        // Find the second bucket for the message
        long now = System.currentTimeMillis();
        int secondStamp = (int)(now / 1000);

        // Insertion offset
        int offset = secondStamp % perSecondMessageRates.length;

        // May need to zero fill gaps - definitely need to zero the new second offset
        if (secondStamp != currentStamp) {
            long timeDiff = secondStamp - currentStamp;

            if (timeDiff >= 60) {
                // More than a minute has elapsed since the last message - zero fill
                Arrays.fill(perSecondMessageRates, 0);
            } else if (timeDiff > 1) {
                // Zero fill the gap - may have wrapped around the end of the array
            }

            int currentOffset = currentStamp % perSecondMessageRates.length;
            if (offset > currentOffset) {
                Arrays.fill(perSecondMessageRates, currentOffset, offset + 1, 0);
            } else {
                // Wrapped - fill from current offset to the end and then from beginning to offset.
                Arrays.fill(perSecondMessageRates, currentOffset, perSecondMessageRates.length, 0);
                Arrays.fill(perSecondMessageRates, 0, offset + 1, 0);
            }

            // Update current stamp.
            currentStamp = secondStamp;
        }

        perSecondMessageRates[offset]++;
        currentMessageRate = currentPerSecondRate();
        packetcount++;
    }

    /**
     * @return moving average per second rate over last minute
     */
    private int currentPerSecondRate() {
        int sum = 0;
        for (int ii = 0; ii < perSecondMessageRates.length; ii++) {
            sum += perSecondMessageRates[ii];
        }
        return sum / perSecondMessageRates.length;
    }

    /**
     * Acquire a message transfer permit.
     * @throws InterruptedException
     */
    void acquire() throws InterruptedException {
        incrementRate();
        permits.acquire();
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

        increment = rate / 50;
        lastAdjustmentTimeStamp = System.nanoTime();
    }

    @Override
    public void close() throws IOException {
        running = false;
    }

    @Override
    public void run() {
        long nextTick = System.nanoTime();
        long minuteTimer = nextTick;
        while (running) {
            try {
                // Check for further throttle easing due to idleness
                if (nextTick > lastAdjustmentTimeStamp + (5*60*1000*1000*1000L)) {
                    // Five minutes since any update - maybe ease.
                    if ((currentMessageRate / rate) > 0.9) {
                        // Current rate is approaching or at the rate limit
                        adjustThrottle(Adjustment.EASE);
                    }
                }

                if (nextTick  > minuteTimer) {
                    System.out.println("Message rate: " + currentMessageRate + " total packets: " + packetcount);
                    minuteTimer += 60*1000*1000*1000L;
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
}
