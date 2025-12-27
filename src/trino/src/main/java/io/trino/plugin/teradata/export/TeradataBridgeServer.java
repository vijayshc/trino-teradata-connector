package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.type.Type;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * High-performance Java Bridge Server that receives data directly from Teradata AMPs.
 * 
 * Optimized Architecture:
 * - Uses AsyncDecompressionPipeline for parallel decompression and parsing.
 * - Uses DirectTrinoPageParser for zero-copy binary-to-page conversion.
 * - Eliminates Apache Arrow to remove overhead.
 */
public class TeradataBridgeServer implements AutoCloseable {
    private static final Logger log = Logger.get(TeradataBridgeServer.class);
    
    // Magic number for control messages
    public static final int CONTROL_MAGIC = 0xFEEDFACE;
    
    // Thread pool limits to prevent OOM from unbounded thread creation
    private static final int CORE_POOL_SIZE = 10;
    private static final int MAX_POOL_SIZE = 200;  // Max concurrent AMP connections
    private static final int QUEUE_CAPACITY = 500; // Backlog before rejecting
    
    private final int port;
    private final int socketReceiveBufferSize;
    private final int inputBufferSize;
    private final ExecutorService executor;
    private ServerSocket serverSocket;
    private final TrinoExportConfig config;
    private volatile boolean running = true;

    @Inject
    public TeradataBridgeServer(TrinoExportConfig config) {
        this.config = config;
        this.port = config.getBridgePort();
        this.socketReceiveBufferSize = config.getSocketReceiveBufferSize();
        this.inputBufferSize = config.getInputBufferSize();
        
        // Use bounded thread pool to prevent memory exhaustion
        this.executor = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "teradata-bridge-handler");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()  // Back-pressure: caller handles if queue full
        );
        
        log.info("TeradataBridgeServer initialized with socketReceiveBufferSize=%d, inputBufferSize=%d, maxThreads=%d",
                socketReceiveBufferSize, inputBufferSize, MAX_POOL_SIZE);
    }

    @PostConstruct
    public void start() {
        executor.submit(this::runServer);
        log.info("Teradata Bridge Server starting on port %d", port);
    }

    private void runServer() {
        try {
            serverSocket = new ServerSocket(port);
            log.info("Teradata Bridge Server listening on port %d", port);
            
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    clientSocket.setReceiveBufferSize(socketReceiveBufferSize);
                    log.info("Connection from %s", clientSocket.getRemoteSocketAddress());
                    executor.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        log.warn("Error accepting connection: %s", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error(e, "Failed to start bridge server on port %d", port);
        }
    }

    private void handleClient(Socket socket) {
        String queryId = "unknown";
        boolean incremented = false;
        long compressedBytes = 0;
        long decompressedBytes = 0;
        int totalRows = 0;
        java.util.zip.Inflater inflater = null;  // Declare outside try for proper cleanup
            
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), inputBufferSize));
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            
            // 1. Security Token Validation
            if (config.getSecurityToken() != null && !config.getSecurityToken().isEmpty()) {
                int tokenLen = in.readInt();
                byte[] tokenBytes = new byte[tokenLen];
                in.readFully(tokenBytes);
                String receivedToken = new String(tokenBytes, StandardCharsets.UTF_8);
                
                if (!config.getSecurityToken().equals(receivedToken)) {
                    log.error("Invalid security token from %s", socket.getRemoteSocketAddress());
                    out.write("ERROR: UNAUTHORIZED".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    return;
                }
            }
            
            // 2. Read Magic Number or Query ID Length
            int lenOrMagic = in.readInt();
            
            if (lenOrMagic == CONTROL_MAGIC) {
                handleControlMessage(in, out);
                return;
            }
            
            // It's a normal Query ID
            byte[] queryIdBytes = new byte[lenOrMagic];
            in.readFully(queryIdBytes);
            queryId = new String(queryIdBytes, StandardCharsets.UTF_8);
            log.info("Receiving data for query: %s", queryId);

            // Register this connection FIRST (before any data processing)
            DataBufferRegistry.incrementConnections(queryId);
            incremented = true;
            
            // Read Compression Flag
            boolean compressionEnabled = in.readInt() == 1;
            if (compressionEnabled) {
                log.info("Compression enabled for query %s", queryId);
            }

            // Read Schema JSON (for verification and name matching)
            int schemaLen = in.readInt();
            byte[] schemaBytes = new byte[schemaLen];
            in.readFully(schemaBytes);
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            
            // Fetch registered Trino Types (Critical for direct parsing)
            List<Type> trinoTypes = DataBufferRegistry.getSchema(queryId);
            if (trinoTypes == null) {
                // Retry once after a delay to handle race condition where Split hasn't registered schema yet
                Thread.sleep(1000);
                trinoTypes = DataBufferRegistry.getSchema(queryId);
                if (trinoTypes == null) {
                    throw new IllegalStateException("No Trino schema registered for query " + queryId + ". PageSource implementation must register schema before data transfer.");
                }
            }
            
            // Create Column Specs using the existing helper method
            List<DirectTrinoPageParser.ColumnSpec> columns = AsyncDecompressionPipeline.parseSchema(schemaJson, trinoTypes);
            
            // Initialize profiler
            PerformanceProfiler.getOrCreate(queryId);
            
            // Synchronous processing - create decompression buffer with dynamic sizing
            // Start with 4MB and grow if needed (reduces GC pressure vs fixed 64MB)
            inflater = compressionEnabled ? new java.util.zip.Inflater() : null;
            byte[] decompressionBuffer = compressionEnabled ? new byte[4 * 1024 * 1024] : null;

            // Read and process batches synchronously until end of stream
            while (true) {
                // Profile: Network Read
                long netStart = System.nanoTime();
                int batchLen = in.readInt();
                if (batchLen == 0) {
                    log.info("End of stream (marker) for query %s", queryId);
                    break;
                }
                
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);
                long netEnd = System.nanoTime();
                PerformanceProfiler.recordNetworkRead(queryId, netEnd - netStart, batchLen);
                compressedBytes += batchLen;
                
                // SYNCHRONOUS: Decompress immediately in this thread
                byte[] decompressed;
                int decompressedLen;
                
                if (compressionEnabled) {
                    long decompStart = System.nanoTime();
                    inflater.reset();
                    inflater.setInput(batchData, 0, batchLen);
                    
                    // Dynamic buffer growth: if buffer too small, allocate larger one
                    // Estimate decompressed size (typical ratio 3-10x)
                    int estimatedSize = batchLen * 10;
                    if (estimatedSize > decompressionBuffer.length) {
                        // Cap at 64MB to prevent OOM from malicious/corrupt data
                        int newSize = Math.min(estimatedSize, 64 * 1024 * 1024);
                        if (newSize > decompressionBuffer.length) {
                            log.debug("Growing decompression buffer from %d to %d bytes", decompressionBuffer.length, newSize);
                            decompressionBuffer = new byte[newSize];
                        }
                    }
                    
                    decompressedLen = inflater.inflate(decompressionBuffer);
                    long decompEnd = System.nanoTime();
                    PerformanceProfiler.recordDecompression(queryId, decompEnd - decompStart, decompressedLen);
                    decompressed = decompressionBuffer;
                    decompressedBytes += decompressedLen;
                } else {
                    decompressed = batchData;
                    decompressedLen = batchLen;
                    decompressedBytes += batchLen;
                }

                // SYNCHRONOUS: Parse directly to Trino Page in this thread
                long parseStart = System.nanoTime();
                io.trino.spi.Page page = DirectTrinoPageParser.parseDirectToPage(decompressed, decompressedLen, columns);
                long parseEnd = System.nanoTime();
                
                if (page != null && page.getPositionCount() > 0) {
                    totalRows += page.getPositionCount();
                    PerformanceProfiler.recordArrowParsing(queryId, parseEnd - parseStart, page.getPositionCount());
                    
                    // SYNCHRONOUS: Push to buffer in this thread
                    // Data is in buffer BEFORE we move to next batch or decrement connection
                    long pushStart = System.nanoTime();
                    DataBufferRegistry.pushData(queryId, page);
                    long pushEnd = System.nanoTime();
                    PerformanceProfiler.recordQueuePush(queryId, pushEnd - pushStart, (pushEnd - pushStart) > 1_000_000);
                }
            }
            
            // All data is now in the buffer - safe to send acknowledgment
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
            
            double ratio = compressedBytes > 0 ? (double) decompressedBytes / compressedBytes : 1.0;
            log.info("Successfully processed query %s: %d rows, %.2f MB compressed, %.2f MB decompressed (Ratio: %.2fx)", 
                queryId, totalRows, compressedBytes / (1024.0 * 1024.0), decompressedBytes / (1024.0 * 1024.0), ratio);
            
        } catch (Exception e) {
            log.error(e, "Error handling client for query %s", queryId);
        } finally {
            // CRITICAL: Release Inflater native memory to prevent native memory leak
            if (inflater != null) {
                try {
                    inflater.end();
                } catch (Exception e) {
                    log.warn("Error ending Inflater for query %s: %s", queryId, e.getMessage());
                }
            }
            
            // CRITICAL: Connection is only decremented AFTER all data is in the buffer
            if (incremented) {
                DataBufferRegistry.decrementConnections(queryId);
            }
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private void handleControlMessage(DataInputStream in, DataOutputStream out) throws IOException {
        String queryId = "unknown";
        try {
            int qidLen = in.readInt();
            byte[] qidBytes = new byte[qidLen];
            in.readFully(qidBytes);
            queryId = new String(qidBytes, StandardCharsets.UTF_8);
            
            int command = in.readInt();
            if (command == 1) { // 1 = JDBC_FINISHED
                log.info("Received Global EOS signal for query %s", queryId);
                DataBufferRegistry.signalJdbcFinished(queryId);
            }
            
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (Exception e) {
            log.error(e, "Error handling control message for query %s", queryId);
        }
    }

    @Override
    @PreDestroy
    public void close() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        executor.shutdownNow();
        log.info("Teradata Bridge Server stopped");
    }
}
