package io.trino.plugin.teradata.export;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom SplitSource that waits for dynamic filter completion before returning splits.
 * This ensures that dynamic filter predicates are fully collected from the build side
 * of a JOIN before the probe side (Teradata) query is executed.
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

        // Check if dynamic filter is complete or if we should wait
        if (config.isEnableDynamicFiltering() && dynamicFilter != null && dynamicFilter.isAwaitable()) {
            if (!dynamicFilter.isComplete()) {
                // Return a future that completes when the dynamic filter is ready
                log.info("Waiting for dynamic filter before returning splits for query %s", splitId);
                CompletableFuture<?> blocked = dynamicFilter.isBlocked().toCompletableFuture();
                return blocked.thenApply(v -> {
                    // Re-check after waiting
                    return createSplitBatchWithTeradataExecution();
                });
            }
        }

        // Dynamic filter is complete (or not applicable), return splits now
        return CompletableFuture.completedFuture(createSplitBatchWithTeradataExecution());
    }

    private ConnectorSplitBatch createSplitBatchWithTeradataExecution() {
        // Only start Teradata execution once
        if (teradataExecutionStarted.compareAndSet(false, true)) {
            log.info("Dynamic filter ready, triggering Teradata execution for query %s", splitId);
            executor.submit(() -> triggerTeradataExecution());
        }

        splitsReturned.set(true);
        return new ConnectorSplitBatch(splits, true);
    }

    private void triggerTeradataExecution() {
        String tableName = tableHandle.getSchemaTableName().toString();
        
        // Log dynamic filter state
        if (dynamicFilter != null) {
            log.info("Dynamic filter state for query %s: isComplete=%s, isAwaitable=%s, isAll=%s",
                    splitId, dynamicFilter.isComplete(), dynamicFilter.isAwaitable(),
                    dynamicFilter.getCurrentPredicate().isAll());
        }

        // Build column list for SELECT - use projected columns if available
        // NOTE: The projected columns from applyProjection already contain the correct
        // base column names (extracted from ColumnHandle.getName()), so we use them as-is.
        String columnList;
        if (tableHandle.getProjectedColumns().isPresent() && !tableHandle.getProjectedColumns().get().isEmpty()) {
            columnList = String.join(", ", tableHandle.getProjectedColumns().get());
            log.info("Using column pruning for query %s: %s", splitId, columnList);
        } else {
            columnList = "*";
        }
        
        // Build WHERE clause
        List<String> predicateParts = new ArrayList<>();
        
        // 1. Static predicates from Metadata.applyFilter
        tableHandle.getPredicateClause().ifPresent(predicateParts::add);
        
        // 2. Dynamic predicates from Trino's DynamicFilter
        if (config.isEnableDynamicFiltering() && dynamicFilter != null && !dynamicFilter.getCurrentPredicate().isAll()) {
            TupleDomain<ColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate();
            log.info("Processing dynamic filter predicate for query %s: %s", splitId, dynamicPredicate);
            
            dynamicPredicate.getDomains().ifPresent(domains -> {
                for (java.util.Map.Entry<ColumnHandle, io.trino.spi.predicate.Domain> entry : domains.entrySet()) {
                    TrinoExportColumnHandle column = (TrinoExportColumnHandle) entry.getKey();
                    String sql = TrinoExportFilterUtils.domainToSql(column.getName(), column.getType(), entry.getValue());
                    if (sql != null) {
                        predicateParts.add(sql);
                        log.info("Dynamic filter predicate for query %s: %s", splitId, sql);
                    }
                }
            });
        }

        String whereClause = predicateParts.isEmpty() ? "" : " WHERE " + String.join(" AND ", predicateParts);
        if (!whereClause.isEmpty()) {
            log.info("Final pushed down WHERE clause for query %s: %s", splitId, whereClause);
        }

        // Build SAMPLE clause from limit pushdown
        String sampleClause = "";
        if (tableHandle.getLimit().isPresent()) {
            sampleClause = " SAMPLE " + tableHandle.getLimit().getAsLong();
            log.info("Using limit pushdown for query %s: %s", splitId, sampleClause);
        }

        // Build the final Teradata SQL using the Table Operator
        String schemaTable = tableHandle.getSchemaTableName().getSchemaName() + "." + 
                             tableHandle.getSchemaTableName().getTableName();
        String innerQuery = "SELECT " + columnList + " FROM " + schemaTable + whereClause + sampleClause;
        
        String teradataSql;
        if (config.getSecurityToken() != null && !config.getSecurityToken().isEmpty()) {
            teradataSql = "SELECT * FROM TrinoExport.ExportToTrino(" +
                    "  ON (" + innerQuery + ")" +
                    "   ON (SELECT '" + targetIps + "' as target_ips, '" + splitId + "' as qid, '" + config.getSecurityToken() + "' as token) DIMENSION" +
                    ") AS export_result";
        } else {
            teradataSql = "SELECT * FROM TrinoExport.ExportToTrino(" +
                    "  ON (" + innerQuery + ")" +
                    "   ON (SELECT '" + targetIps + "' as target_ips, '" + splitId + "' as qid) DIMENSION" +
                    ") AS export_result";
        }

        log.info("Executing Teradata SQL for query %s: %s", splitId, teradataSql);

        try (java.sql.Connection conn = getConnection();
             java.sql.Statement stmt = conn.createStatement()) {
            java.sql.ResultSet rs = stmt.executeQuery(teradataSql);
            while (rs.next()) {
                // Process stats returned by the UDF
            }
            rs.close();
            log.info("Teradata SQL execution finished successfully for query %s", splitId);
        } catch (Exception e) {
            log.error(e, "Error executing Teradata SQL for query %s", splitId);
        } finally {
            DataBufferRegistry.signalJdbcFinished(splitId);
        }
    }

    private java.sql.Connection getConnection() throws Exception {
        java.sql.Driver driver = (java.sql.Driver) Class.forName("com.teradata.jdbc.TeraDriver")
                .getDeclaredConstructor().newInstance();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", config.getTeradataUser());
        props.setProperty("password", config.getTeradataPassword());
        java.sql.Connection conn = driver.connect(config.getTeradataUrl(), props);
        
        // Proxy Authentication: Set QUERY_BAND to identify the Trino user to Teradata
        // This allow Teradata to perform authorization checks based on the Trino login user.
        if (trinoUser != null && !trinoUser.isEmpty()) {
            String qb = "PROXYUSER=" + trinoUser + ";";
            log.info("Setting Teradata PROXYUSER for query %s: %s", splitId, trinoUser);
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("SET QUERY_BAND = '" + qb + "' FOR SESSION;");
            } catch (java.sql.SQLException e) {
                log.warn("Failed to set QUERY_BAND for query %s: %s", splitId, e.getMessage());
            }
        }
        
        return conn;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public boolean isFinished() {
        return closed.get() || splitsReturned.get();
    }
}
