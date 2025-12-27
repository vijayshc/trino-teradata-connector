package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TrinoExportPageSource implements ConnectorPageSource {
    private static final Logger log = Logger.get(TrinoExportPageSource.class);
    
    private final String queryId;
    private final BlockingQueue<BatchContainer> buffer;
    private final List<TrinoExportColumnHandle> columns;
    private final java.time.ZoneOffset teradataZoneOffset;
    private final java.time.ZoneOffset localZoneOffset;
    private final long pagePollTimeoutMs;
    private final boolean enableDebugLogging;
    private long completedBytes = 0;
    private boolean finished = false;

    public TrinoExportPageSource(String queryId, List<ColumnHandle> columns, String teradataTimezone, 
                                  long pagePollTimeoutMs, boolean enableDebugLogging) {
        this.queryId = queryId;
        
        // CRITICAL: Register buffer on THIS worker (multi-worker support)
        // Each worker needs its own buffer registration since DataBufferRegistry is per-JVM
        DataBufferRegistry.registerQuery(queryId);
        this.buffer = DataBufferRegistry.getBuffer(queryId);
        
        // Register this instance as a consumer to support parallel split processing
        DataBufferRegistry.incrementConsumers(queryId);
        
        this.columns = columns.stream()
                .map(TrinoExportColumnHandle.class::cast)
                .collect(Collectors.toList());

        // CRITICAL: Register Schema Types so the Bridge Server can use Direct Parsing
        List<io.trino.spi.type.Type> types = this.columns.stream()
                .map(TrinoExportColumnHandle::getType)
                .collect(Collectors.toList());
        DataBufferRegistry.registerSchema(queryId, types);
        
        this.pagePollTimeoutMs = pagePollTimeoutMs;
        this.enableDebugLogging = enableDebugLogging;
        
        // Parse Teradata timezone offset
        this.teradataZoneOffset = java.time.ZoneOffset.of(teradataTimezone);
        // Get local timezone offset
        this.localZoneOffset = java.time.ZoneId.systemDefault().getRules().getOffset(java.time.Instant.now());
        
        log.debug("PageSource created for query %s. Registered %d column types.", queryId, types.size());
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }

        try {
            // Profile: Queue Poll
            long pollStart = System.nanoTime();
            BatchContainer container = buffer.poll(pagePollTimeoutMs, TimeUnit.MILLISECONDS);
            long pollEnd = System.nanoTime();
            boolean waited = container == null || (pollEnd - pollStart) > 1_000_000;
            PerformanceProfiler.recordQueuePoll(queryId, pollEnd - pollStart, waited);
            
            if (container == null) {
                return null;
            }

            if (container.isEndOfStream()) {
                log.debug("Received end of stream for query %s", queryId);
                finished = true;
                // Re-push the marker so other parallel consumers for the same query can also finish
                DataBufferRegistry.pushEndMarker(queryId);
                // Generate and log performance profile summary
                PerformanceProfiler.generateSummary(queryId);
                return null;
            }

            // Direct Zero-Copy Pass-Through
            Page page = container.page();
            
            if (page != null) {
                completedBytes += page.getSizeInBytes();
                return SourcePage.create(page);
            }
            return null;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finished = true;
            return null;
        }

    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        // CRITICAL: Guaranteed cleanup - must succeed even if partial failure
        try {
            DataBufferRegistry.deregisterQuery(queryId);
        } catch (Exception e) {
            // Log but don't throw - cleanup must complete
            log.warn("Error during cleanup for query %s: %s", queryId, e.getMessage());
        }
    }
}
