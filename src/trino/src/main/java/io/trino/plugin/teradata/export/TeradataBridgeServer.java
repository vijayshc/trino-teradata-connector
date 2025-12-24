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
 * 
 * This eliminates the need for an external Python bridge process, providing:
 * - Zero-copy data transfer within the JVM
 * - Native Java NIO for high-throughput network I/O
 * - Direct Arrow buffer allocation in Trino's memory space
 * 
 * Protocol (from Teradata C UDF):
 * 1. Query ID: 4-byte length (big-endian) + UTF-8 string
 * 2. Schema JSON: 4-byte length (big-endian) + JSON string
 * 3. Batches: 4-byte length (big-endian) + batch data (0 = end of stream)
 * 
 * Batch format:
 * - 4 bytes: row count
 * - For each row, for each column:
 *   - 1 byte: null indicator (1=null, 0=not null)
 *   - If not null: type-specific data
 */
public class TeradataBridgeServer implements AutoCloseable {
    private static final Logger log = Logger.get(TeradataBridgeServer.class);
    
    private final int port;
    private final BufferAllocator allocator;
    private final ExecutorService executor;
    private ServerSocket serverSocket;
    private final TrinoExportConfig config;
    private volatile boolean running = true;

    @Inject
    public TeradataBridgeServer(TrinoExportConfig config) {
        this.config = config;
        this.port = config.getBridgePort();
        this.allocator = new RootAllocator();
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "teradata-bridge-handler");
            t.setDaemon(true);
            return t;
        });
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
                    clientSocket.setReceiveBufferSize(4 * 1024 * 1024); // 4MB receive buffer
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
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1024 * 1024));
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            
            // 1. Security Token Validation
            if (config.getSecurityToken() != null && !config.getSecurityToken().isEmpty()) {
                int tokenLen = in.readInt();
                byte[] tokenBytes = new byte[tokenLen];
                in.readFully(tokenBytes);
                String receivedToken = new String(tokenBytes, StandardCharsets.UTF_8);
                
                if (!config.getSecurityToken().equals(receivedToken)) {
                    log.error("Invalid security token from %s. Expected: %s, Received: %s", 
                            socket.getRemoteSocketAddress(), config.getSecurityToken(), receivedToken);
                    out.write("ERROR: UNATHORIZED".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    return;
                }
            }
            
            // 2. Read Query ID
            int queryIdLen = in.readInt();
            byte[] queryIdBytes = new byte[queryIdLen];
            in.readFully(queryIdBytes);
            queryId = new String(queryIdBytes, StandardCharsets.UTF_8);
            log.info("Receiving data for query: %s", queryId);

            // Register this connection
            DataBufferRegistry.incrementConnections(queryId);
            
            // Read Schema JSON
            int schemaLen = in.readInt();
            byte[] schemaBytes = new byte[schemaLen];
            in.readFully(schemaBytes);
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            
            // Parse schema and create Arrow schema
            List<ColumnInfo> columns = parseSchema(schemaJson);
            Schema arrowSchema = createArrowSchema(columns);
            
            int totalRows = 0;
            int batchCount = 0;
            
            // Read batches until end of stream
            while (true) {
                int batchLen = in.readInt();
                if (batchLen == 0) {
                    log.info("End of stream for query %s", queryId);
                    break;
                }
                
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);
                
                // Parse batch and create Arrow vectors
                VectorSchemaRoot root = parseBatch(batchData, columns, arrowSchema);
                totalRows += root.getRowCount();
                batchCount++;
                
                // Push to DataBufferRegistry
                DataBufferRegistry.pushData(queryId, root);
            }
            
            // Send OK response
            out.write("OK".getBytes(StandardCharsets.UTF_8));
            out.flush();
            
            log.info("Successfully received %d rows in %d batches for query %s", totalRows, batchCount, queryId);
            
        } catch (Exception e) {
            log.error(e, "Error handling client for query %s", queryId);
        } finally {
            // Decrement connection count - may trigger EOS if all connections done
            DataBufferRegistry.decrementConnections(queryId);
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private List<ColumnInfo> parseSchema(String json) {
        // Simple JSON parsing for schema: {"columns":[{"name":"col_0","type":"VARCHAR"},...]}}
        List<ColumnInfo> columns = new ArrayList<>();
        
        int columnsStart = json.indexOf("[");
        int columnsEnd = json.lastIndexOf("]");
        if (columnsStart < 0 || columnsEnd < 0) {
            return columns;
        }
        
        String columnsJson = json.substring(columnsStart + 1, columnsEnd);
        
        // Parse each column
        int pos = 0;
        while (pos < columnsJson.length()) {
            int objStart = columnsJson.indexOf("{", pos);
            if (objStart < 0) break;
            int objEnd = columnsJson.indexOf("}", objStart);
            if (objEnd < 0) break;
            
            String colJson = columnsJson.substring(objStart, objEnd + 1);
            
            // Extract name
            String name = extractJsonString(colJson, "name");
            String type = extractJsonString(colJson, "type");
            
            if (name != null && type != null) {
                columns.add(new ColumnInfo(name, type));
            }
            
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
                case "INTEGER" -> new ArrowType.Int(32, true);
                case "BIGINT" -> new ArrowType.Int(64, true);
                case "DOUBLE" -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
                default -> new ArrowType.Utf8(); // VARCHAR and others
            };
            fields.add(new Field(col.name, org.apache.arrow.vector.types.pojo.FieldType.nullable(type), null));
        }
        return new Schema(fields);
    }

    private VectorSchemaRoot parseBatch(byte[] data, List<ColumnInfo> columns, Schema schema) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        
        ByteBuffer buf = ByteBuffer.wrap(data);
        int numRows = buf.getInt();
        
        // Allocate vectors
        for (FieldVector vector : root.getFieldVectors()) {
            vector.allocateNew();
        }
        
        // Parse rows
        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < columns.size(); col++) {
                FieldVector vector = root.getVector(col);
                boolean isNull = buf.get() == 1;
                
                if (isNull) {
                    setNull(vector, row);
                } else {
                    setValue(vector, row, buf, columns.get(col).type);
                }
            }
        }
        
        root.setRowCount(numRows);
        return root;
    }

    private void setNull(FieldVector vector, int row) {
        if (vector instanceof IntVector iv) {
            iv.setNull(row);
        } else if (vector instanceof BigIntVector bv) {
            bv.setNull(row);
        } else if (vector instanceof Float8Vector fv) {
            fv.setNull(row);
        } else if (vector instanceof VarCharVector vv) {
            vv.setNull(row);
        }
    }

    private void setValue(FieldVector vector, int row, ByteBuffer buf, String type) {
        switch (type) {
            case "INTEGER" -> {
                int val = buf.getInt();
                ((IntVector) vector).setSafe(row, val);
            }
            case "BIGINT" -> {
                long val = buf.getLong();
                ((BigIntVector) vector).setSafe(row, val);
            }
            case "DOUBLE" -> {
                double val = buf.getDouble();
                ((Float8Vector) vector).setSafe(row, val);
            }
            default -> {
                // VARCHAR - 2 bytes length + data
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
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        executor.shutdownNow();
        allocator.close();
        log.info("Teradata Bridge Server stopped");
    }

    private record ColumnInfo(String name, String type) {}
}
