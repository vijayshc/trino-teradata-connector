package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * High-performance Java Bridge Server that receives data directly from Teradata AMPs.
 */
public class TeradataBridgeServer implements AutoCloseable {
    private static final Logger log = Logger.get(TeradataBridgeServer.class);
    
    // Magic number for control messages
    public static final int CONTROL_MAGIC = 0xFEEDFACE;
    
    private final int port;
    private final int socketReceiveBufferSize;
    private final int inputBufferSize;
    private final BufferAllocator allocator;
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
        this.allocator = new RootAllocator();
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "teradata-bridge-handler");
            t.setDaemon(true);
            return t;
        });
        
        log.info("TeradataBridgeServer initialized with socketReceiveBufferSize=%d, inputBufferSize=%d",
                socketReceiveBufferSize, inputBufferSize);
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

            // Register this connection
            DataBufferRegistry.incrementConnections(queryId);
            incremented = true;
            // Read Compression Flag
            boolean compressionEnabled = in.readInt() == 1;
            if (compressionEnabled) {
                log.info("Compression enabled for query %s", queryId);
            }

            // Read Schema JSON
            int schemaLen = in.readInt();
            byte[] schemaBytes = new byte[schemaLen];
            in.readFully(schemaBytes);
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            
            List<ColumnInfo> columns = parseSchema(schemaJson);
            Schema arrowSchema = createArrowSchema(columns);
            
            int totalRows = 0;
            java.util.zip.Inflater inflater = compressionEnabled ? new java.util.zip.Inflater() : null;
            byte[] decompressionBuffer = compressionEnabled ? new byte[64 * 1024 * 1024] : null;

            // Read batches until end of stream
            while (true) {
                int batchLen = in.readInt();
                if (batchLen == 0) {
                    log.info("End of stream (marker) for query %s", queryId);
                    break;
                }
                
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);
                
                byte[] processedData = batchData;
                if (compressionEnabled) {
                    inflater.reset();
                    inflater.setInput(batchData);
                    int decompressedLen = inflater.inflate(decompressionBuffer);
                    processedData = java.util.Arrays.copyOf(decompressionBuffer, decompressedLen);
                }

                VectorSchemaRoot root = parseBatch(processedData, columns, arrowSchema);
                totalRows += root.getRowCount();
                DataBufferRegistry.pushData(queryId, root);
            }
            
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
            log.info("Successfully received %d rows for query %s", totalRows, queryId);
            
        } catch (Exception e) {
            log.error(e, "Error handling client for query %s", queryId);
        } finally {
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

    private List<ColumnInfo> parseSchema(String json) {
        List<ColumnInfo> columns = new ArrayList<>();
        int columnsStart = json.indexOf("[");
        int columnsEnd = json.lastIndexOf("]");
        if (columnsStart < 0 || columnsEnd < 0) return columns;
        
        String columnsJson = json.substring(columnsStart + 1, columnsEnd);
        int pos = 0;
        while (pos < columnsJson.length()) {
            int objStart = columnsJson.indexOf("{", pos);
            if (objStart < 0) break;
            int objEnd = columnsJson.indexOf("}", objStart);
            if (objEnd < 0) break;
            String colJson = columnsJson.substring(objStart, objEnd + 1);
            String name = extractJsonString(colJson, "name");
            String type = extractJsonString(colJson, "type");
            if (name != null && type != null) columns.add(new ColumnInfo(name, type));
            pos = objEnd + 1;
        }
        return columns;
    }

    private String extractJsonString(String json, String key) {
        String searchKey = "\"" + key + "\":\"";
        int start = json.indexOf(searchKey);
        if (start < 0) return null;
        start += searchKey.length();
        int end = json.indexOf("\"", start);
        if (end < 0) return null;
        return json.substring(start, end);
    }

    private Schema createArrowSchema(List<ColumnInfo> columns) {
        List<Field> fields = new ArrayList<>();
        for (ColumnInfo col : columns) {
            ArrowType type = switch (col.type) {
                case "INTEGER", "DATE" -> new ArrowType.Int(32, true);
                case "BIGINT", "TIME", "TIMESTAMP", "DECIMAL_SHORT" -> new ArrowType.Int(64, true);
                case "DOUBLE" -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
                case "DECIMAL_LONG" -> new ArrowType.FixedSizeBinary(16);
                default -> new ArrowType.Utf8();
            };
            fields.add(new Field(col.name, org.apache.arrow.vector.types.pojo.FieldType.nullable(type), null));
        }
        return new Schema(fields);
    }

    private VectorSchemaRoot parseBatch(byte[] data, List<ColumnInfo> columns, Schema schema) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int numRows = buf.getInt();
            for (FieldVector vector : root.getFieldVectors()) vector.allocateNew();
            for (int row = 0; row < numRows; row++) {
                for (int col = 0; col < columns.size(); col++) {
                    FieldVector vector = root.getVector(col);
                    if (buf.get() == 1) setNull(vector, row);
                    else setValue(vector, row, buf, columns.get(col).type);
                }
            }
            root.setRowCount(numRows);
            return root;
        } catch (Exception e) {
            root.close();
            throw e;
        }
    }

    private void setNull(FieldVector vector, int row) {
        if (vector instanceof IntVector iv) iv.setNull(row);
        else if (vector instanceof BigIntVector bv) bv.setNull(row);
        else if (vector instanceof Float8Vector fv) fv.setNull(row);
        else if (vector instanceof VarCharVector vv) vv.setNull(row);
        else if (vector instanceof FixedSizeBinaryVector fsv) fsv.setNull(row);
    }

    private void setValue(FieldVector vector, int row, ByteBuffer buf, String type) {
        switch (type) {
            case "INTEGER", "DATE" -> ((IntVector) vector).setSafe(row, buf.getInt());
            case "BIGINT", "TIME", "TIMESTAMP", "DECIMAL_SHORT" -> ((BigIntVector) vector).setSafe(row, buf.getLong());
            case "DOUBLE" -> ((Float8Vector) vector).setSafe(row, buf.getDouble());
            case "DECIMAL_LONG" -> {
                byte[] bytes = new byte[16];
                buf.get(bytes);
                ((FixedSizeBinaryVector) vector).setSafe(row, bytes);
            }
            default -> {
                int len = buf.getShort() & 0xFFFF;
                byte[] strBytes = new byte[len];
                buf.get(strBytes);
                ((VarCharVector) vector).setSafe(row, strBytes);
            }
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
        allocator.close();
        log.info("Teradata Bridge Server stopped");
    }

    private record ColumnInfo(String name, String type) {}
}
