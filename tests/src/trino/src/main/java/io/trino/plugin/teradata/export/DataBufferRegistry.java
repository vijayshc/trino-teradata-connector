package io.trino.plugin.teradata.export;

import org.apache.arrow.vector.VectorSchemaRoot;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import io.airlift.log.Logger;

/**
 * Global registry for buffering Arrow batches received from Teradata.
 * Each query has its own isolated buffer identified by QueryID.
 * 
 * IMPORTANT: This registry is static but per-JVM. In a multi-worker Trino cluster,
 * each worker has its own isolated instance. The design ensures:
 * 1. Each worker registers its own buffer when PageSource is created
 * 2. Each worker receives data only from AMPs assigned to it
 * 3. EOS is detected when all socket connections close AND JDBC is finished
 */
public class DataBufferRegistry {
    private static final Logger log = Logger.get(DataBufferRegistry.class);
    private static final Map<String, QueryBuffer> queryBuffers = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "data-buffer-eos-scheduler");
        t.setDaemon(true);
        return t;
    });
    
    // Configurable queue capacity (set from TrinoExportConfig during initialization)
    private static int bufferQueueCapacity = 100;  // Default value

    private static class QueryBuffer {
        final BlockingQueue<BatchContainer> queue;
        final AtomicInteger activeConnections = new AtomicInteger(0);
        volatile boolean jdbcFinished = false;
        volatile boolean eosSignaled = false;
        final long createdAt = System.currentTimeMillis();
        volatile long lastActivityTime = System.currentTimeMillis();

        QueryBuffer(int capacity) {
            this.queue = new LinkedBlockingQueue<>(capacity);
        }

        /**
         * Update the last activity timestamp to prevent premature EOS.
         */
        void updateActivity() {
            this.lastActivityTime = System.currentTimeMillis();
        }

        /**
         * Check and signal end-of-stream.
         * Logic: Signal EOS when all connections are done AND JDBC finished signal received
         * AND a short stabilization period has passed since last activity.
         */
        synchronized void checkAndSignalEos(String queryId) {
            if (eosSignaled) return;
            
            if (jdbcFinished && activeConnections.get() == 0) {
                long now = System.currentTimeMillis();
                long idleTime = now - lastActivityTime;
                
                // Stabilization: wait at least 500ms after last activity or creation
                // to ensure no sockets are in the OS accept queue or in-flight.
                if (idleTime < 500) {
                    log.debug("EOS check deferred for query %s (idle: %d ms). Scheduling retry.", queryId, idleTime);
                    scheduler.schedule(() -> checkAndSignalEos(queryId), 500, TimeUnit.MILLISECONDS);
                    return;
                }

                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.info("Signaled end of stream for query %s (confirmed: All sockets closed, JDBC finished, and idle > 500ms)", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                log.debug("EOS check for query %s: jdbcFinished=%b, activeConnections=%d", 
                        queryId, jdbcFinished, activeConnections.get());
            }
        }
        
        synchronized void forceSignalEos(String queryId) {
            if (!eosSignaled) {
                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.info("Force signaled end of stream for query %s", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void setBufferQueueCapacity(int capacity) {
        bufferQueueCapacity = capacity;
        log.info("DataBufferRegistry queue capacity set to %d", capacity);
    }

    public static int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    public static void registerQuery(String queryId) {
        queryBuffers.computeIfAbsent(queryId, k -> {
            log.info("Registered buffer for query %s (capacity: %d)", queryId, bufferQueueCapacity);
            return new QueryBuffer(bufferQueueCapacity);
        });
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
    
    public static BlockingQueue<BatchContainer> getOrCreateBuffer(String queryId) {
        registerQuery(queryId);
        return getBuffer(queryId);
    }

    public static void incrementConnections(String queryId) {
        registerQuery(queryId);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.updateActivity();
            int count = buffer.activeConnections.incrementAndGet();
            log.debug("Incremented connections for query %s: now %d", queryId, count);
        }
    }

    public static void decrementConnections(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            int count = buffer.activeConnections.decrementAndGet();
            log.debug("Decremented connections for query %s: now %d", queryId, count);
            buffer.checkAndSignalEos(queryId);
        }
    }

    /**
     * Mark that JDBC execution is finished globally.
     * This may trigger EOS if all connections are already closed.
     */
    public static void signalJdbcFinished(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            log.info("JDBC execution finished signal received for query %s", queryId);
            buffer.jdbcFinished = true;
            buffer.checkAndSignalEos(queryId);
        }
    }
    
    public static boolean hasBuffer(String queryId) {
        return queryBuffers.containsKey(queryId);
    }

    public static void pushData(String queryId, VectorSchemaRoot root) {
        registerQuery(queryId);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.updateActivity();
            try {
                log.debug("Pushing batch with %d rows for query %s", root.getRowCount(), queryId);
                buffer.queue.put(BatchContainer.of(root));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                root.close();
            }
        } else {
            root.close();
        }
    }

    public static void shutdown() {
        log.info("Shutting down DataBufferRegistry...");
        scheduler.shutdownNow();
        for (String queryId : queryBuffers.keySet()) {
            deregisterQuery(queryId);
        }
        queryBuffers.clear();
    }
}
