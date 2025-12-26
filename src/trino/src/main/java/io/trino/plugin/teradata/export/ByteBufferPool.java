package io.trino.plugin.teradata.export;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Thread-safe pool of reusable ByteBuffers to reduce GC pressure.
 * Option E: Zero-Copy ByteBuffer Pooling
 */
public final class ByteBufferPool {
    private final ArrayBlockingQueue<ByteBuffer> pool;
    private final int bufferSize;

    public ByteBufferPool(int poolSize, int bufferSize) {
        this.pool = new ArrayBlockingQueue<>(poolSize);
        this.bufferSize = bufferSize;
        // Pre-allocate buffers
        for (int i = 0; i < poolSize; i++) {
            pool.offer(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public ByteBuffer acquire() {
        ByteBuffer buffer = pool.poll();
        if (buffer == null) {
            // Pool exhausted, allocate new buffer (will be GC'd)
            return ByteBuffer.allocateDirect(bufferSize);
        }
        buffer.clear();
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if (buffer != null && buffer.capacity() == bufferSize) {
            buffer.clear();
            pool.offer(buffer); // May fail if pool is full, that's OK
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
