package io.trino.plugin.teradata.export;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import java.util.List;
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
 * MEMORY MANAGEMENT: All cleanup is deterministic and integrated into query lifecycle:
 * - deregisterQuery(): Called when PageSource closes (normal completion)
 * - cleanupOnFailure(): Called when JDBC execution fails (error path)
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
    private static final Map<String, List<Type>> schemaRegistry = new ConcurrentHashMap<>();
    
    // Scheduler for short-lived EOS timing checks only (ms-scale delays, not TTL cleanup)
    private static final ScheduledExecutorService eosScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "data-buffer-eos-scheduler");
        t.setDaemon(true);
        return t;
    });
    
    // Configurable queue capacity (set from TrinoExportConfig during initialization)
    private static int bufferQueueCapacity = 100;  // Default value
    
    // NOTE: No TTL-based cleanup - all cleanup is deterministic via deregisterQuery/cleanupOnFailure

    private static class QueryBuffer {
        final BlockingQueue<BatchContainer> queue;
        final AtomicInteger activeConnections = new AtomicInteger(0);
        volatile boolean jdbcFinished = false;
        volatile boolean eosSignaled = false;
        volatile boolean hadAnyConnections = false;  // Track if we ever saw a connection
        final long createdAt = System.currentTimeMillis();
        volatile long lastActivityTime = System.currentTimeMillis();
        final AtomicInteger activeConsumers = new AtomicInteger(0);
        
        // Error signaling for immediate failure propagation
        volatile Exception queryError = null;
        volatile String errorMessage = null;

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
         * 
         * With SYNCHRONOUS processing in TeradataBridgeServer:
         * - All data is pushed to buffer BEFORE connection is decremented
         * - When activeConnections reaches 0, we know ALL data is in the buffer
         * - We just need a short stabilization to handle any in-flight socket accepts
         */
        synchronized void checkAndSignalEos(String queryId) {
            if (eosSignaled) return;
            
            if (jdbcFinished && activeConnections.get() == 0 && hadAnyConnections) {
                // Data is guaranteed to be in buffer (synchronous processing)
                // Just a minimal stabilization period for socket accept race conditions
                long now = System.currentTimeMillis();
                long idleTime = now - lastActivityTime;
                
                if (idleTime < 100) {
                    log.debug("EOS check deferred for query %s (stabilizing: %d ms)", queryId, idleTime);
                    eosScheduler.schedule(() -> checkAndSignalEos(queryId), 100, TimeUnit.MILLISECONDS);
                    return;
                }

                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.debug("Signaled end of stream for query %s (connections completed, data in buffer)", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else if (jdbcFinished && !hadAnyConnections) {
                // Edge case: JDBC finished but no socket connections ever arrived
                // This could happen for queries that return no data, or network issues
                long now = System.currentTimeMillis();
                long timeSinceCreation = now - createdAt;
                
                // Wait up to 5 seconds for connections, then give up
                if (timeSinceCreation < 5000) {
                    log.debug("EOS check deferred for query %s (waiting for connections, age: %d ms)", queryId, timeSinceCreation);
                    eosScheduler.schedule(() -> checkAndSignalEos(queryId), 500, TimeUnit.MILLISECONDS);
                    return;
                }
                
                // No connections after 5 seconds - assume query returned no data
                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.debug("Signaled end of stream for query %s (no connections received, assuming empty result)", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                log.debug("EOS check for query %s: jdbcFinished=%b, activeConnections=%d, hadConnections=%b", 
                        queryId, jdbcFinished, activeConnections.get(), hadAnyConnections);
            }
        }
        
        synchronized void forceSignalEos(String queryId) {
            if (!eosSignaled) {
                try {
                    queue.put(BatchContainer.endOfStream());
                    eosSignaled = true;
                    log.debug("Force signaled end of stream for query %s", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void setBufferQueueCapacity(int capacity) {
        bufferQueueCapacity = capacity;
        log.debug("DataBufferRegistry queue capacity set to %d", capacity);
    }

    public static int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    public static void registerQuery(String queryId) {
        queryBuffers.computeIfAbsent(queryId, k -> {
            log.debug("Registered buffer for query %s (capacity: %d)", queryId, bufferQueueCapacity);
            return new QueryBuffer(bufferQueueCapacity);
        });
    }

    public static void incrementConsumers(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.activeConsumers.incrementAndGet();
        }
    }

    public static void deregisterQuery(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            int remaining = buffer.activeConsumers.decrementAndGet();
            if (remaining > 0) {
                log.debug("Consumer closed for query %s. %d consumers remaining.", queryId, remaining);
                return;
            }
            
            queryBuffers.remove(queryId);
            // Also clean up schema registry and performance profiler entries
            schemaRegistry.remove(queryId);
            PerformanceProfiler.clear(queryId);
            
            log.debug("Deregistered buffer, schema, and profiler for query %s. All consumers closed.", queryId);
            while (!buffer.queue.isEmpty()) {
                BatchContainer container = buffer.queue.poll();
                if (container != null && !container.isEndOfStream()) {
                    try {
                    // Page doesn't need explicit closing like Arrow
                    // But if Page had native resources they would be released here
                    container = null;
                    } catch (Exception e) {
                        log.warn("Error during cleanup for query %s: %s", queryId, e.getMessage());
                    }
                }
            }
        }
    }
    
    // NOTE: cleanupStaleBuffers removed - all cleanup is deterministic via deregisterQuery/cleanupOnFailure

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
            buffer.hadAnyConnections = true;  // Mark that we received at least one connection
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
            log.debug("JDBC execution finished signal received for query %s", queryId);
            buffer.jdbcFinished = true;
            buffer.checkAndSignalEos(queryId);
        }
    }

    /**
     * PROACTIVE CLEANUP: Called when JDBC execution fails.
     * This cleans up immediately instead of waiting for TTL-based cleanup.
     * This prevents memory accumulation when queries fail before any data flows.
     * 
     * Only cleans up if there are no active consumers (PageSources) using the buffer.
     * If there are consumers, they will clean up when they close.
     */
    public static void cleanupOnFailure(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer == null) {
            // Buffer was never created or already cleaned up
            return;
        }
        
        // Only clean up if no consumers are using this buffer
        // If consumers exist, let them clean up via deregisterQuery on close()
        if (buffer.activeConsumers.get() > 0) {
            log.debug("Skipping immediate cleanup for query %s: %d active consumers will clean up on close", 
                    queryId, buffer.activeConsumers.get());
            return;
        }
        
        // No consumers - proactively clean up now
        log.warn("Proactive cleanup on failure for query %s (no active consumers)", queryId);
        
        // Force signal EOS in case anything is waiting
        buffer.forceSignalEos(queryId);
        
        // Remove from all registries
        queryBuffers.remove(queryId);
        schemaRegistry.remove(queryId);
        PerformanceProfiler.clear(queryId);
        
        // Clear the queue
        buffer.queue.clear();
    }
    
    public static boolean hasBuffer(String queryId) {
        return queryBuffers.containsKey(queryId);
    }

    public static void registerSchema(String queryId, List<Type> types) {
        schemaRegistry.put(queryId, types);
        log.debug("Registered schema types for query %s: %s", queryId, types);
    }

    public static List<Type> getSchema(String queryId) {
        return schemaRegistry.get(queryId);
    }

    public static void pushData(String queryId, Page page) {
        registerQuery(queryId);
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.updateActivity();
            try {
                // log.debug("Pushing batch with %d rows for query %s", page.getPositionCount(), queryId);
                buffer.queue.put(BatchContainer.of(page));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void pushEndMarker(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            try {
                buffer.queue.put(BatchContainer.endOfStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void shutdown() {
        log.debug("Shutting down DataBufferRegistry...");
        eosScheduler.shutdownNow();
        
        // Clean up all remaining buffers
        for (String queryId : queryBuffers.keySet()) {
            QueryBuffer buffer = queryBuffers.get(queryId);
            if (buffer != null) {
                buffer.forceSignalEos(queryId);
                buffer.queue.clear();
            }
        }
        queryBuffers.clear();
        schemaRegistry.clear();
        log.debug("DataBufferRegistry shutdown complete. Cleared all buffers and schemas.");
    }
}
