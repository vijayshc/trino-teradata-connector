package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom SplitSource that waits for dynamic filter completion before returning splits
 * and broadcasts EOS signal after Teradata execution is done.
 */
public class TrinoExportDynamicFilteringSplitSource implements ConnectorSplitSource {
    private static final Logger log = Logger.get(TrinoExportDynamicFilteringSplitSource.class);

    private final List<ConnectorSplit> splits;
    private final DynamicFilter dynamicFilter;
    private final TrinoExportTableHandle tableHandle;
    private final String splitId;
    private final String targetIps;
    private final String trinoUser;
    private final TrinoExportConfig config;
    private final ExecutorService executor;
    // connectionPool removed
    
    private final AtomicBoolean teradataExecutionStarted = new AtomicBoolean(false);
    private final AtomicBoolean splitsReturned = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TrinoExportDynamicFilteringSplitSource(
            List<ConnectorSplit> splits,
            DynamicFilter dynamicFilter,
            TrinoExportTableHandle tableHandle,
            String splitId,
            String targetIps,
            String trinoUser,
            TrinoExportConfig config,
            ExecutorService executor) {
        this.splits = new ArrayList<>(splits);
        this.dynamicFilter = dynamicFilter;
        this.tableHandle = tableHandle;
        this.splitId = splitId;
        this.targetIps = targetIps;
        this.trinoUser = trinoUser;
        this.config = config;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
        if (closed.get() || splitsReturned.get()) {
            return CompletableFuture.completedFuture(new ConnectorSplitBatch(List.of(), true));
        }

        if (config.isEnableDynamicFiltering() && dynamicFilter != null && dynamicFilter.isAwaitable()) {
            if (!dynamicFilter.isComplete()) {
                log.info("Waiting for dynamic filter for query %s", splitId);
                CompletableFuture<?> blocked = dynamicFilter.isBlocked().toCompletableFuture();
                return blocked.thenApply(v -> createSplitBatchWithTeradataExecution());
            }
        }

        return CompletableFuture.completedFuture(createSplitBatchWithTeradataExecution());
    }

    private ConnectorSplitBatch createSplitBatchWithTeradataExecution() {
        if (teradataExecutionStarted.compareAndSet(false, true)) {
            log.info("Triggering Teradata execution for query %s", splitId);
            executor.submit(this::triggerTeradataExecution);
        }
        splitsReturned.set(true);
        return new ConnectorSplitBatch(splits, true);
    }

    private void triggerTeradataExecution() {
        // Determine the SELECT list based on whether aggregation is pushed down
        String columnList;
        String groupByClause = "";
        
        if (tableHandle.hasAggregation()) {
            TrinoExportTableHandle.AggregationInfo agg = tableHandle.getAggregation().get();
            // Use projected columns if present, otherwise use all columns defined during aggregation pushdown
            if (tableHandle.getProjectedColumns().isPresent()) {
                columnList = agg.toSelectList(tableHandle.getProjectedColumns().get());
            } else {
                columnList = agg.toSelectList();
            }
            groupByClause = agg.toGroupByClause();
            log.info("Using aggregation pushdown for query %s: SELECT %s %s", splitId, columnList, groupByClause);
        } else {
            columnList = tableHandle.getProjectedColumns()
                    .filter(c -> !c.isEmpty())
                    .map(c -> String.join(", ", c))
                    .orElse("*");
        }
        
        List<String> predicateParts = new ArrayList<>();
        tableHandle.getPredicateClause().ifPresent(predicateParts::add);
        
        if (config.isEnableDynamicFiltering() && dynamicFilter != null && !dynamicFilter.getCurrentPredicate().isAll()) {
            TupleDomain<ColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate();
            dynamicPredicate.getDomains().ifPresent(domains -> {
                for (java.util.Map.Entry<ColumnHandle, io.trino.spi.predicate.Domain> entry : domains.entrySet()) {
                    TrinoExportColumnHandle column = (TrinoExportColumnHandle) entry.getKey();
                    String sql = TrinoExportFilterUtils.domainToSql(column.getName(), column.getType(), entry.getValue());
                    if (sql != null) predicateParts.add(sql);
                }
            });
        }

        String whereClause = predicateParts.isEmpty() ? "" : " WHERE " + String.join(" AND ", predicateParts);
        
        // For Top-N queries: use "TOP N" with ORDER BY (sorted result)
        // For plain LIMIT: use SAMPLE (random sampling, no sorting)
        // Note: Aggregation queries typically don't use LIMIT pushdown in the same way
        String topClause = "";
        String sampleClause = "";
        String orderByClause = "";
        
        // For Top-N queries: use "TOP N" with ORDER BY (sorted result)
        // For plain LIMIT: use SAMPLE (random sampling, no sorting)
        if (tableHandle.isTopN()) {
            // Top-N: ORDER BY ... LIMIT N -> SELECT TOP N ... ORDER BY ...
            topClause = " TOP " + tableHandle.getLimit().getAsLong();
            orderByClause = " ORDER BY " + TrinoExportTableHandle.SortItem.toSqlString(tableHandle.getSortOrder().get());
            log.info("Using Top-N pushdown for query %s: TOP %d ORDER BY ...", splitId, tableHandle.getLimit().getAsLong());
        } else if (tableHandle.getLimit().isPresent()) {
            // Plain LIMIT without ORDER BY -> SAMPLE (random rows)
            sampleClause = " SAMPLE " + tableHandle.getLimit().getAsLong();
            log.info("Using SAMPLE pushdown for query %s: SAMPLE %d", splitId, tableHandle.getLimit().getAsLong());
        }
        
        String schemaTable = tableHandle.getSchemaTableName().getSchemaName() + "." + 
                             tableHandle.getSchemaTableName().getTableName();
        String innerQuery = "SELECT" + topClause + " " + columnList + " FROM " + schemaTable + 
                           whereClause + groupByClause + orderByClause + sampleClause;
        
        // SECURITY: Generate a random, per-query token to replace the static token in query logs.
        String dynamicToken = UUID.randomUUID().toString();
        DataBufferRegistry.registerDynamicToken(splitId, dynamicToken);
        
        String compressionAlgorithmName = "NONE";
        if (config.isCompressionEnabled()) {
            compressionAlgorithmName = config.getCompressionAlgorithm().name();
        }

        String teradataSql = String.format(
                "SELECT * FROM %s.%s(" +
                "  ON (%s)" +
                "   ON (SELECT CAST('%s' AS VARCHAR(2048)) as target_ips, CAST('%s' AS VARCHAR(256)) as qid, CAST('%s' AS VARCHAR(256)) as token, CAST(%d AS INTEGER) as batch_size, CAST('%s' AS VARCHAR(20)) as compression_algorithm) DIMENSION" +
                ") AS export_result", 
                config.getUdfDatabase(), config.getUdfName(), innerQuery, targetIps, splitId, dynamicToken, config.getBatchSize(), compressionAlgorithmName);

        // SECURITY: Mask the dynamic token in logged SQL
        String logSql = teradataSql.replace(dynamicToken, "***DYNAMIC_TOKEN***");
        log.info("Executing Teradata SQL for query %s: %s", splitId, logSql);

        // Direct connection (no pool)
        try (Connection conn = getConnection();
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(teradataSql)) {
            while (rs.next()) { }
            log.info("Teradata SQL execution finished successfully for query %s", splitId);
        } catch (Exception e) {
            log.error(e, "Error executing Teradata SQL for query %s", splitId);
            DataBufferRegistry.cleanupOnFailure(splitId);
        } finally {
            // IMPORTANT: Broadcast BEFORE signaling JDBC finished to avoid race condition
            // where EOS cleanup removes the token before broadcast retrieves it.
            // Pass the token directly since we already have it.
            broadcastJdbcFinishedSignal(dynamicToken);
            DataBufferRegistry.signalJdbcFinished(splitId);
        }
    }

    private void broadcastJdbcFinishedSignal(String dynamicToken) {
        if (targetIps == null || targetIps.isEmpty()) return;
        
        String[] ips = targetIps.split(",");
        for (String ipPort : ips) {
            String target = ipPort.trim();
            if (target.isEmpty()) continue;
            
            try {
                String[] parts = target.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                log.info("Broadcasting Finished signal to worker %s for query %s", target, splitId);
                try (Socket socket = new Socket(host, port);
                     DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
                    
                    socket.setSoTimeout(5000);
                    
                    // 1. Dynamic Token (passed directly to avoid race condition with cleanup)
                    if (dynamicToken != null) {
                        byte[] tokenBytes = dynamicToken.getBytes(StandardCharsets.UTF_8);
                        out.writeInt(tokenBytes.length);
                        out.write(tokenBytes);
                    } else {
                        log.warn("No dynamic token available for broadcast to %s for query %s", target, splitId);
                    }
                    
                    // 2. Control Magic
                    out.writeInt(TeradataBridgeServer.CONTROL_MAGIC);
                    
                    // 3. Query ID
                    byte[] qidBytes = splitId.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(qidBytes.length);
                    out.write(qidBytes);
                    
                    // 4. Command (1 = JDBC_FINISHED)
                    out.writeInt(1);
                    out.flush();
                }
            } catch (Exception e) {
                log.warn("Failed to broadcast Finished signal to worker %s: %s", target, e.getMessage());
            }
        }
    }

    private Connection getConnection() throws SQLException {
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Teradata JDBC Driver not found", e);
        }
        
        Properties props = new Properties();
        props.setProperty("user", config.getTeradataUser());
        props.setProperty("password", config.getTeradataPassword());
        
        Connection conn = DriverManager.getConnection(config.getTeradataUrl(), props);
        
        if (trinoUser != null && !trinoUser.isEmpty()) {
            String qb = "PROXYUSER=" + trinoUser + ";";
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("SET QUERY_BAND = '" + qb + "' FOR SESSION;");
            } catch (SQLException e) {
                if (config.isEnforceProxyAuthentication()) {
                    conn.close();
                    throw new SQLException("Proxy authentication failed: " + e.getMessage(), e);
                }
                log.warn("Proxy authentication warning: %s", e.getMessage());
            }
        }
        
        return conn;
    }

    @Override
    public void close() { closed.set(true); }

    @Override
    public boolean isFinished() { return closed.get() || splitsReturned.get(); }
}
