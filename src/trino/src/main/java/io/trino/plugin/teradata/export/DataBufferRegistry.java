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
    private static final Map<String, BlockingQueue<BatchContainer>> queryBuffers = new ConcurrentHashMap<>();

    public static void registerQuery(String queryId) {
        queryBuffers.putIfAbsent(queryId, new LinkedBlockingQueue<>(100));
        log.info("Registered buffer for query %s", queryId);
    }

    public static void deregisterQuery(String queryId) {
        BlockingQueue<BatchContainer> queue = queryBuffers.remove(queryId);
        if (queue != null) {
            log.info("Deregistering and clearing buffer for query %s. Remaining batches: %d", queryId, queue.size());
            while (!queue.isEmpty()) {
                BatchContainer container = queue.poll();
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
        return queryBuffers.get(queryId);
    }

    public static void pushData(String queryId, VectorSchemaRoot root) {
        BlockingQueue<BatchContainer> queue = getBuffer(queryId);
        if (queue != null) {
            try {
                log.info("Pushing batch with %d rows for query %s", root.getRowCount(), queryId);
                queue.put(BatchContainer.of(root));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            log.warn("No buffer found for query %s", queryId);
        }
    }

    public static void signalEndOfStream(String queryId) {
        BlockingQueue<BatchContainer> queue = getBuffer(queryId);
        if (queue != null) {
            try {
                log.info("Signaling end of stream for query %s", queryId);
                queue.put(BatchContainer.endOfStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
