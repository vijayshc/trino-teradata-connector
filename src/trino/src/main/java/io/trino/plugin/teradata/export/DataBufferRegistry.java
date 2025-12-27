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
    
    // Dynamic per-query token storage for security
    private static final Map<String, String> dynamicTokenRegistry = new ConcurrentHashMap<>();
    
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
        
        // Multi-split tracking
        final AtomicInteger totalFinishedConsumers = new AtomicInteger(0);
        volatile int expectedConsumers = 1;
        
        // Error signaling for immediate failure propagation
        volatile Exception queryError = null;
        volatile String errorMessage = null;

        QueryBuffer(int capacity, int expectedConsumers) {
            this.queue = new LinkedBlockingQueue<>(capacity);
            this.expectedConsumers = expectedConsumers;
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
                    // CRITICAL: Push EOS markers for ALL consumers
                    // Each PageSource (split) needs its own EOS marker to finish
                    int consumerCount = Math.max(1, activeConsumers.get());
                    for (int i = 0; i < consumerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    eosSignaled = true;
                    log.debug("Signaled end of stream for query %s (pushed %d EOS markers for %d consumers)", 
                            queryId, consumerCount, consumerCount);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else if (jdbcFinished && !hadAnyConnections) {
                // Edge case: JDBC finished but no socket connections ever arrived on this worker.
                // In multi-node setups, this is NORMAL for queries returning few records (e.g., max(date)).
                // Since jdbcFinished is true, we know the query is complete - no more data will arrive.
                // Only wait a short stabilization period (200ms) for any in-flight socket accepts.
                long now = System.currentTimeMillis();
                long timeSinceJdbcFinished = now - lastActivityTime;  // lastActivityTime updated on JDBC finished
                
                // Short stabilization: 200ms is enough for any in-flight socket accepts
                // The 5-second wait was causing unnecessary delay for workers that correctly received no data
                if (timeSinceJdbcFinished < 200) {
                    log.debug("EOS check deferred for query %s (short stabilization: %d ms)", queryId, timeSinceJdbcFinished);
                    eosScheduler.schedule(() -> checkAndSignalEos(queryId), 100, TimeUnit.MILLISECONDS);
                    return;
                }
                
                // JDBC finished and stabilization complete - signal EOS immediately
                try {
                    // Push EOS markers for ALL consumers
                    int consumerCount = Math.max(1, activeConsumers.get());
                    for (int i = 0; i < consumerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    eosSignaled = true;
                    log.debug("Signaled end of stream for query %s (no data for this worker, pushed %d EOS markers)", 
                            queryId, consumerCount);
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
                    int consumerCount = Math.max(1, activeConsumers.get());
                    for (int i = 0; i < consumerCount; i++) {
                        queue.put(BatchContainer.endOfStream());
                    }
                    eosSignaled = true;
                    log.debug("Force signaled end of stream for query %s (pushed %d EOS markers)", queryId, consumerCount);
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
        registerQuery(queryId, 1);
    }

    public static void registerQuery(String queryId, int expectedConsumers) {
        queryBuffers.compute(queryId, (k, v) -> {
            if (v == null) {
                log.debug("Registered buffer for query %s (capacity: %d, consumers: %d)", queryId, bufferQueueCapacity, expectedConsumers);
                return new QueryBuffer(bufferQueueCapacity, expectedConsumers);
            } else {
                // Buffer exists, update expectation just in case
                v.expectedConsumers = Math.max(v.expectedConsumers, expectedConsumers);
                return v;
            }
        });
    }

    public static void incrementConsumers(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            buffer.activeConsumers.incrementAndGet();
            
            // CRITICAL: Handle late joiners (lazy split creation)
            // If EOS was already signaled, this new consumer missed the original EOS push.
            // We must push a new EOS marker just for this consumer.
            if (buffer.eosSignaled) {
                try {
                    buffer.queue.put(BatchContainer.endOfStream());
                    log.debug("Late consumer registered for query %s - pushed immediate EOS marker", queryId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while pushing EOS for late consumer of query %s", queryId);
                }
            }
        }
    }

    public static void deregisterQuery(String queryId) {
        QueryBuffer buffer = queryBuffers.get(queryId);
        if (buffer != null) {
            int remainingActive = buffer.activeConsumers.decrementAndGet();
            int finished = buffer.totalFinishedConsumers.incrementAndGet();
            
            // Only clean up when ALL expected consumers have finished
            // This handles lazy split execution where activeConsumers might drop to 0 before the next split starts
            if (finished < buffer.expectedConsumers) {
                log.debug("Consumer closed for query %s. %d/%d finished. %d active.", 
                        queryId, finished, buffer.expectedConsumers, remainingActive);
                return;
            }
            
            queryBuffers.remove(queryId);
            // Also clean up schema registry, token registry, and performance profiler entries
            schemaRegistry.remove(queryId);
            dynamicTokenRegistry.remove(queryId);
            PerformanceProfiler.clear(queryId);
            
            log.debug("Deregistered buffer, schema, and profiler for query %s. All %d consumers finished.", queryId, finished);
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
            buffer.updateActivity();  // Update timestamp so stabilization timing is accurate
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
        dynamicTokenRegistry.remove(queryId);
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

    /**
     * Register a dynamic token for a query.
     * This token is generated per-query and used for socket authentication.
     */
    public static void registerDynamicToken(String queryId, String token) {
        dynamicTokenRegistry.put(queryId, token);
        log.debug("Registered dynamic token for query %s", queryId);
    }

    /**
     * Validate a dynamic token for a query.
     * @return true if the token matches the registered token for this query
     */
    public static boolean validateDynamicToken(String queryId, String receivedToken) {
        String expectedToken = dynamicTokenRegistry.get(queryId);
        if (expectedToken == null) {
            log.warn("No dynamic token registered for query %s", queryId);
            return false;
        }
        boolean valid = expectedToken.equals(receivedToken);
        if (!valid) {
            log.error("Invalid dynamic token for query %s", queryId);
        }
        return valid;
    }

    /**
     * Get the dynamic token for a query.
     */
    public static String getDynamicToken(String queryId) {
        return dynamicTokenRegistry.get(queryId);
    }

    /**
     * Check if a dynamic token is registered for a query.
     */
    public static boolean hasDynamicToken(String queryId) {
        return dynamicTokenRegistry.containsKey(queryId);
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
        dynamicTokenRegistry.clear();
        log.debug("DataBufferRegistry shutdown complete. Cleared all buffers, schemas, and tokens.");
    }
}
