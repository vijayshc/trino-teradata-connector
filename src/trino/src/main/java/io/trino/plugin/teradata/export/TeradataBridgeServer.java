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
    
    // Thread pool limits - now controlled by config with defaults
    private static final int CORE_POOL_SIZE = 10;
    
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
        int maxThreads = config.getMaxBridgeThreads();
        int queueCapacity = config.getBridgeQueueCapacity();
        this.executor = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                maxThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                r -> {
                    Thread t = new Thread(r, "teradata-bridge-handler");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()  // Back-pressure: caller handles if queue full
        );
        
        log.info("TeradataBridgeServer initialized with socketReceiveBufferSize=%d, inputBufferSize=%d, maxThreads=%d, queueCapacity=%d",
                socketReceiveBufferSize, inputBufferSize, maxThreads, queueCapacity);
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
            
            // 1. Mandatory Dynamic Token Validation
            // Protocol: [tokenLen (int)][token (string)][queryIdLen/Magic (int)][queryId (string)]...
            int tokenLen = in.readInt();
            if (tokenLen <= 0 || tokenLen > 1024) {
                log.error("Invalid token length: %d from %s", tokenLen, socket.getRemoteSocketAddress());
                return;
            }
            byte[] tokenBytes = new byte[tokenLen];
            in.readFully(tokenBytes);
            String receivedToken = new String(tokenBytes, StandardCharsets.UTF_8);
            
            // 2. Read Magic Number or Query ID Length
            int lenOrMagic = in.readInt();
            
            if (lenOrMagic == CONTROL_MAGIC) {
                handleControlMessage(in, out, receivedToken);
                return;
            }
            
            // It's a normal Query ID - Validate length to prevent NegativeArraySizeException
            if (lenOrMagic <= 0 || lenOrMagic > 1024) {
                log.error("Invalid Query ID length or Magic Number: %d from %s", lenOrMagic, socket.getRemoteSocketAddress());
                return;
            }
            byte[] queryIdBytes = new byte[lenOrMagic];
            in.readFully(queryIdBytes);
            queryId = new String(queryIdBytes, StandardCharsets.UTF_8);
            
            // Now we have both QueryId and Token - Validate it
            if (!DataBufferRegistry.validateDynamicToken(queryId, receivedToken)) {
                log.error("Unauthorized: Invalid dynamic token for query %s from %s", queryId, socket.getRemoteSocketAddress());
                out.write("ERROR: UNAUTHORIZED".getBytes(StandardCharsets.UTF_8));
                out.flush();
                return;
            }

            log.info("Receiving data for authenticated query: %s", queryId);

            // Register this connection FIRST (before any data processing)
            DataBufferRegistry.incrementConnections(queryId);
            incremented = true;
            
            // Read Compression Type
            int compressionType = in.readInt();
            String algo = (compressionType == 2) ? "LZ4" : (compressionType == 1) ? "ZLIB" : "NONE";
            if (compressionType != 0) {
                log.info("AUTHENTICATION SUCCESS: Query %s using compression %s", queryId, algo);
            } else {
                log.info("AUTHENTICATION SUCCESS: Query %s with compression DISABLED", queryId);
            }

            // Read Schema JSON (for verification and name matching)
            int schemaLen = in.readInt();
            byte[] schemaBytes = new byte[schemaLen];
            in.readFully(schemaBytes);
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            
            // Fetch registered Trino Types (Critical for direct parsing)
            List<Type> trinoTypes = null;
            for (int i = 0; i < 20; i++) { // Wait up to 10 seconds (20 * 500ms)
                trinoTypes = DataBufferRegistry.getSchema(queryId);
                if (trinoTypes != null) break;
                Thread.sleep(500);
            }
            if (trinoTypes == null) {
                throw new IllegalStateException("No Trino schema registered for query " + queryId + ". PageSource implementation must register schema before data transfer.");
            }
            
            // Create Column Specs using the existing helper method
            List<DirectTrinoPageParser.ColumnSpec> columns = AsyncDecompressionPipeline.parseSchema(schemaJson, trinoTypes);
            
            // Initialize profiler
            PerformanceProfiler.getOrCreate(queryId);
            
            // Synchronous processing - create decompression buffer with enough space for max Teradata batch (16MB)
            // Using 32MB to be absolutely safe and avoid reallocations
            inflater = (compressionType == 1) ? new java.util.zip.Inflater() : null;
            io.airlift.compress.lz4.Lz4Decompressor lz4Decompressor = (compressionType == 2) ? new io.airlift.compress.lz4.Lz4Decompressor() : null;
            byte[] decompressionBuffer = (compressionType != 0) ? new byte[32 * 1024 * 1024] : null;

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
                
                if (compressionType == 1) { /* ZLIB */
                    long decompStart = System.nanoTime();
                    inflater.reset();
                    inflater.setInput(batchData, 0, batchLen);
                    
                    // Ensure buffer is large enough for decompression. 
                    // Max Teradata batch is 16MB, so 32MB should always be enough.
                    if (decompressionBuffer.length < 32 * 1024 * 1024) {
                        decompressionBuffer = new byte[32 * 1024 * 1024];
                    }
                    
                    decompressedLen = inflater.inflate(decompressionBuffer);
                    long decompEnd = System.nanoTime();
                    PerformanceProfiler.recordDecompression(queryId, decompEnd - decompStart, decompressedLen);
                    decompressed = decompressionBuffer;
                    decompressedBytes += decompressedLen;
                } else if (compressionType == 2) { /* LZ4 */
                    long decompStart = System.nanoTime();
                    // Ensure buffer is large enough.
                    if (decompressionBuffer.length < 32 * 1024 * 1024) {
                        decompressionBuffer = new byte[32 * 1024 * 1024];
                    }
                    decompressedLen = lz4Decompressor.decompress(batchData, 0, batchLen, decompressionBuffer, 0, decompressionBuffer.length);
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
                    PerformanceProfiler.recordDirectParsing(queryId, parseEnd - parseStart, page.getPositionCount());
                    
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

    private void handleControlMessage(DataInputStream in, DataOutputStream out, String receivedToken) throws IOException {
        String queryId = "unknown";
        try {
            int qidLen = in.readInt();
            byte[] qidBytes = new byte[qidLen];
            in.readFully(qidBytes);
            queryId = new String(qidBytes, StandardCharsets.UTF_8);
            
            int command = in.readInt();
            if (command == 1) { // 1 = JDBC_FINISHED
                // For JDBC_FINISHED control messages:
                // - We're the ones who sent this message (from split-executor)
                // - The token was passed directly from triggerTeradataExecution()
                // - The token might not be in registry anymore if PageSource closed
                // - We still validate format to prevent spoofing, but don't require registry lookup
                
                // Validate token format (UUID format: 36 chars with hyphens)
                if (receivedToken == null || receivedToken.length() != 36) {
                    log.warn("Invalid token format for JDBC_FINISHED control message, query %s", queryId);
                    // Still process the signal - this is an internal control flow
                }
                
                log.info("Received Global EOS signal for query %s", queryId);
                DataBufferRegistry.signalJdbcFinished(queryId);
            } else {
                // For other control commands, require full token validation
                if (!DataBufferRegistry.validateDynamicToken(queryId, receivedToken)) {
                    log.error("Unauthorized control message: Invalid dynamic token for query %s", queryId);
                    return;
                }
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
