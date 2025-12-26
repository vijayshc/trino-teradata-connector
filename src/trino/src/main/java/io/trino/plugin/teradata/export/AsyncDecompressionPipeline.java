package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

/**
 * High-performance async decompression pipeline.
 * Option D: Asynchronous Decompression Pipeline
 * 
 * Architecture:
 * [Network Reader] -> CompressedQueue -> [Decompression Pool] -> DecompressedQueue -> [Parser Pool] -> PageQueue
 */
public final class AsyncDecompressionPipeline {
    private static final Logger log = Logger.get(AsyncDecompressionPipeline.class);

    private final String queryId;
    private final List<DirectTrinoPageParser.ColumnSpec> columns;
    private final boolean compressionEnabled;
    private final ByteBufferPool bufferPool;

    // Queues for async pipeline
    private final BlockingQueue<CompressedBatch> compressedQueue;
    private final BlockingQueue<Page> pageQueue;

    // Thread pools
    private final ExecutorService decompressionPool;
    private final AtomicLong compressedBytes = new AtomicLong(0);
    private final AtomicLong decompressedBytes = new AtomicLong(0);
    private final AtomicLong totalRows = new AtomicLong(0);

    private volatile boolean finished = false;
    private volatile Throwable error = null;

    // Inflater per thread (thread-local for thread safety)
    private static final ThreadLocal<Inflater> inflaters = 
            ThreadLocal.withInitial(Inflater::new);

    public record CompressedBatch(byte[] data, int length, boolean isEndMarker) {}

    public AsyncDecompressionPipeline(
            String queryId,
            List<DirectTrinoPageParser.ColumnSpec> columns,
            boolean compressionEnabled,
            int queueCapacity,
            int decompressorThreads) {

        this.queryId = queryId;
        this.columns = columns;
        this.compressionEnabled = compressionEnabled;
        this.compressedQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.pageQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.bufferPool = new ByteBufferPool(decompressorThreads * 2, 64 * 1024 * 1024);

        this.decompressionPool = Executors.newFixedThreadPool(decompressorThreads, r -> {
            Thread t = new Thread(r, "async-decompress-" + queryId.substring(Math.max(0, queryId.length() - 8)));
            t.setDaemon(true);
            return t;
        });

        // Start decompression workers
        for (int i = 0; i < decompressorThreads; i++) {
            decompressionPool.submit(this::decompressAndParseWorker);
        }
    }

    /**
     * Submit compressed batch for async processing.
     * Called by network reader thread.
     */
    public void submitCompressedBatch(byte[] data, int length) throws InterruptedException {
        compressedQueue.put(new CompressedBatch(data, length, false));
        compressedBytes.addAndGet(length);
    }

    /**
     * Signal end of data stream.
     */
    public void signalEndOfStream() throws InterruptedException {
        compressedQueue.put(new CompressedBatch(null, 0, true));
    }

    /**
     * Get next parsed Page. Returns null when finished.
     */
    public Page pollPage(long timeoutMs) throws InterruptedException {
        if (finished && pageQueue.isEmpty()) {
            return null;
        }
        return pageQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean isFinished() {
        return finished && pageQueue.isEmpty();
    }

    public Throwable getError() {
        return error;
    }

    public long getCompressedBytes() {
        return compressedBytes.get();
    }

    public long getDecompressedBytes() {
        return decompressedBytes.get();
    }

    public long getTotalRows() {
        return totalRows.get();
    }

    private void decompressAndParseWorker() {
        byte[] decompressionBuffer = new byte[64 * 1024 * 1024];
        
        try {
            while (!finished) {
                CompressedBatch batch = compressedQueue.poll(100, TimeUnit.MILLISECONDS);
                if (batch == null) continue;

                if (batch.isEndMarker()) {
                    finished = true;
                    // Push end marker through for other workers
                    compressedQueue.offer(batch);
                    break;
                }

                byte[] decompressed;
                int decompressedLen;

                if (compressionEnabled) {
                    Inflater inflater = inflaters.get();
                    inflater.reset();
                    inflater.setInput(batch.data, 0, batch.length);
                    decompressedLen = inflater.inflate(decompressionBuffer);
                    decompressed = decompressionBuffer;
                } else {
                    decompressed = batch.data;
                    decompressedLen = batch.length;
                }

                decompressedBytes.addAndGet(decompressedLen);

                // Parse directly to Trino Page (Option F)
                Page page = DirectTrinoPageParser.parseDirectToPage(decompressed, decompressedLen, columns);
                if (page != null) {
                    totalRows.addAndGet(page.getPositionCount());
                    pageQueue.put(page);
                }
            }
        } catch (Exception e) {
            error = e;
            log.error(e, "Error in async decompression worker for query %s", queryId);
        }
    }

    public void shutdown() {
        finished = true;
        decompressionPool.shutdownNow();
    }

    /**
     * Parse column specs from schema JSON.
     */
    public static List<DirectTrinoPageParser.ColumnSpec> parseSchema(String json, List<Type> trinoTypes) {
        List<DirectTrinoPageParser.ColumnSpec> columns = new ArrayList<>();
        int columnsStart = json.indexOf("[");
        int columnsEnd = json.lastIndexOf("]");
        if (columnsStart < 0 || columnsEnd < 0) return columns;

        String columnsJson = json.substring(columnsStart + 1, columnsEnd);
        int pos = 0;
        int typeIdx = 0;
        while (pos < columnsJson.length()) {
            int objStart = columnsJson.indexOf("{", pos);
            if (objStart < 0) break;
            int objEnd = columnsJson.indexOf("}", objStart);
            if (objEnd < 0) break;
            String colJson = columnsJson.substring(objStart, objEnd + 1);
            String name = extractJsonString(colJson, "name");
            String type = extractJsonString(colJson, "type");
            if (name != null && type != null && typeIdx < trinoTypes.size()) {
                columns.add(new DirectTrinoPageParser.ColumnSpec(name, type, trinoTypes.get(typeIdx)));
                typeIdx++;
            }
            pos = objEnd + 1;
        }
        return columns;
    }

    private static String extractJsonString(String json, String key) {
        String searchKey = "\"" + key + "\":\"";
        int start = json.indexOf(searchKey);
        if (start < 0) return null;
        start += searchKey.length();
        int end = json.indexOf("\"", start);
        if (end < 0) return null;
        return json.substring(start, end);
    }
}
