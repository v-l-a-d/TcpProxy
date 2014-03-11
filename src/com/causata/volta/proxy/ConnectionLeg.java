package com.causata.volta.proxy;

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * A connection leg, either inbound or outbound. A leg is always one of a pair.
 */
class ConnectionLeg {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectionLeg.class);

    private final ByteBuffer buffer;
    private final SocketChannel channel;
    private final SelectionKey key;
    private ConnectionLeg otherLeg;

    /**
     * Constructor for received socket channel
     * @param channel
     */
    ConnectionLeg(SocketChannel channel, SelectionKey key) {
        this.channel = channel;
        this.key = key;
        this.buffer = ByteBuffer.allocateDirect(256 * 1024);  // 256KB
    }

    void setOtherLeg(ConnectionLeg leg) {
        this.otherLeg = leg;
    }

    ConnectionLeg getOtherLeg() {
        return this.otherLeg;
    }

    SocketChannel getChannel() {
        return this.channel;
    }

    /**
     * Read from the channel and write to the other leg
     * @return {@code true} if all the read data is written
     * @throws IOException
     */
    boolean read() {
        boolean complete = false;

        if (this.channel.isConnected()) {
            try {
                // Read data into the receive buffer.
                int lread = this.channel.read(this.buffer);
                if (lread == -1) {
                    // The socket has been closed by the far-end.
                    close();
                } else {
                    // Call the other leg to write the data.
                    this.buffer.flip();
                    complete = this.otherLeg.write(this.buffer);

                    if (this.buffer.remaining() == 0) {
                        this.buffer.clear();
                    }
                    else {
                        // Move remaining data to the front of the buffer
                        this.buffer.compact();
                    }
                }
            }
            catch (IOException iox) {
                close();
            }
        }
        return complete;
    }

    /**
     * Write pending data to the socket channel
     * @return {@code true} if all data in the buffer is written, otherwise {@code false}
     */
    boolean write(ByteBuffer buffer) {
        if (!channel.isConnected()) {
            return false;
        }

        // Write the buffer to the socket channel.
        try {
            channel.write(buffer);

            if (buffer.remaining() == 0) {
                // Wrote the entire buffer - set the selection key's interest set to read operations only.
                //System.out.println("wrote whole buffer");
                key.interestOps(SelectionKey.OP_READ);
                return true;
            } else {
                // Did not write the entire buffer - break out of the loop. Update the selection key set in order to
                // be notified by the selector when the socket becomes writable.
                LOG.debug("wrote part buffer");

                this.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                return false;
            }
        }
        catch (IOException iox) {
            close();
        }
        return true;
    }

    void close() {
        LOG.debug("Closing connection to {}", channel.socket().getRemoteSocketAddress());
        Closeables.closeQuietly(this.channel);
        if (this.otherLeg != null) {
            SocketChannel otherChannel = this.otherLeg.getChannel();
            if (otherChannel.isOpen()) {
                Closeables.closeQuietly(otherChannel);
            }
        }
    }
}
