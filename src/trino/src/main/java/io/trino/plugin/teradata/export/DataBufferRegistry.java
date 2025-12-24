package io.trino.plugin.teradata.export;

import org.apache.arrow.vector.VectorSchemaRoot;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import io.airlift.log.Logger;

/**
 * Global registry for buffering Arrow batches received from Teradata.
 * Each query has its own isolated buffer identified by QueryID.
 */
public class DataBufferRegistry {
    private static final Logger log = Logger.get(DataBufferRegistry.class);
    private static final Map<String, QueryBuffer> queryBuffers = new ConcurrentHashMap<>();

    private static class QueryBuffer {
        final BlockingQueue<BatchContainer> queue = new LinkedBlockingQueue<>(100);
        final java.util.concurrent.atomic.AtomicInteger activeConnections = new java.util.concurrent.atomic.AtomicInteger(0);
        volatile boolean jdbcFinished = false;
        volatile boolean eosSignaled = false;

        synchronized void checkAndSignalEos(String queryId) {
            if (!eosSignaled && jdbcFinished && activeConnections.get() == 0) {
                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.info("Signaled end of stream for query %s", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void registerQuery(String queryId) {
        queryBuffers.putIfAbsent(queryId, new QueryBuffer());
        log.info("Registered buffer for query %s", queryId);
    }

    public static void deregisterQuery(String queryId) {
        QueryBuffer buffer = queryBuffers.remove(queryId);
        if (buffer != null) {
            log.info("Deregistering and clearing buffer for query %s. Remaining batches: %d", queryId, buffer.queue.size());
            while (!buffer.queue.isEmpty()) {
                BatchContainer container = buffer.queue.poll();
                if (container != null && !container.isEndOfStream()) {
                    try {
                        container.root().close();
                    } catch (Exception e) {
                        log.warn("Error closing Arrow root during cleanup for query %s: %s", queryId, e.getMessage());
                    }
                }
            }
        }
    }

    public static BlockingQueue<BatchContainer> getBuffer(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        return buffer != null ? buffer.queue : null;
    }

    public static void incrementConnections(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.activeConnections.incrementAndGet();
        }
    }

    public static void decrementConnections(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.activeConnections.decrementAndGet();
            buffer.checkAndSignalEos(queryId);
        }
    }

    public static void signalJdbcFinished(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.jdbcFinished = true;
            log.info("JDBC execution finished for query %s", queryId);
            buffer.checkAndSignalEos(queryId);
        }
    }

    public static void pushData(String queryId, VectorSchemaRoot root) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            try {
                log.info("Pushing batch with %d rows for query %s", root.getRowCount(), queryId);
                buffer.queue.put(BatchContainer.of(root));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            log.warn("No buffer found for query %s", queryId);
            root.close(); // Important to avoid memory leak
        }
    }
}
