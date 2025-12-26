package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.NodeManager;
import io.trino.spi.Node;

import javax.inject.Inject;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import java.sql.Driver;
import java.util.Properties;

import javax.annotation.PreDestroy;

public class TrinoExportSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(TrinoExportSplitManager.class);
    private final NodeManager nodeManager;
    private final TrinoExportConfig config;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Inject
    public TrinoExportSplitManager(NodeManager nodeManager, TrinoExportConfig config) {
        this.nodeManager = nodeManager;
        this.config = config;
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
        log.info("TrinoExportSplitManager executor shutdown");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        
        List<Node> workers = new ArrayList<>(nodeManager.getWorkerNodes());
        if (workers.isEmpty()) {
            workers.add(nodeManager.getCurrentNode());
        }
        
        // Generate a unique split ID for this specific table within the query
        // This is critical for JOIN queries where multiple tables are queried with the same session query ID
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) table;
        String baseQueryId = session.getQueryId();
        String trinoUser = session.getUser();
        log.info("Generating splits for query %s, user: %s, workers: %d", baseQueryId, trinoUser, workers.size());
        String tableName = tableHandle.getSchemaTableName().toString();
        
        // Create a unique split ID: queryId_tableHash_randomSuffix
        String tableHash = Integer.toHexString(tableName.hashCode() & 0x7FFFFFFF);
        String randomSuffix = Long.toHexString(System.nanoTime() & 0xFFFFF);
        String splitId = baseQueryId + "_" + tableHash + "_" + randomSuffix;
        
        // Note: Don't register query here for multi-worker - let each worker's PageSource register
        // DataBufferRegistry.registerQuery(splitId); 

        // Build worker IPs for Teradata to connect to
        // For multi-worker clusters: resolve hostnames to IPs for Teradata's inet_pton()
        // For single-worker: use configured address (handles loopback/NAT issues)
        String allWorkerIps = buildWorkerIpList(workers);

        log.info("Registering split %s for table %s (query %s). Worker IPs: %s", splitId, tableName, baseQueryId, allWorkerIps);

        // Create splits - multiple per worker for parallel consumption
        List<ConnectorSplit> splits = new ArrayList<>();
        int splitsPerWorker = config.getSplitsPerWorker();
        for (Node node : workers) {
            for (int i = 0; i < splitsPerWorker; i++) {
                splits.add(new TrinoExportSplit(node.getHost(), splitId, allWorkerIps));
            }
        }

        // Use custom SplitSource that waits for dynamic filter completion
        // This is critical for proper dynamic filter pushdown to Teradata
        return new TrinoExportDynamicFilteringSplitSource(
                splits, dynamicFilter, tableHandle, splitId, allWorkerIps, session.getUser(), config, executor);
    }
    
    /**
     * Build comma-separated list of worker IP:port addresses for Teradata.
     * 
     * Priority:
     * 1. If advertised-addresses is configured, use those (for NAT/multi-homed)
     * 2. For multi-worker: resolve hostnames to IPs
     * 3. For single-worker: use configured trino-address
     */
    private String buildWorkerIpList(List<Node> workers) {
        // Check for explicitly configured advertised addresses (for NAT/multi-homed networks)
        String advertisedAddresses = config.getWorkerAdvertisedAddresses();
        if (advertisedAddresses != null && !advertisedAddresses.isEmpty()) {
            log.info("Using configured advertised addresses: %s", advertisedAddresses);
            return advertisedAddresses;
        }
        
        if (workers.size() > 1) {
            // Multi-worker: resolve hostnames to IPs for Teradata's inet_pton()
            return workers.stream()
                    .map(node -> resolveToIp(node.getHost()) + ":" + config.getBridgePort())
                    .distinct()
                    .collect(Collectors.joining(","));
        } else {
            // Single-worker: use configured address (handles loopback issues)
            return config.getTrinoAddress() + ":" + config.getBridgePort();
        }
    }
    
    /**
     * Resolve hostname to IP address for Teradata compatibility.
     * Teradata C UDF uses inet_pton() which requires IP addresses, not hostnames.
     */
    private String resolveToIp(String host) {
        try {
            InetAddress addr = InetAddress.getByName(host);
            String ip = addr.getHostAddress();
            log.debug("Resolved %s -> %s", host, ip);
            return ip;
        } catch (Exception e) {
            log.warn("Failed to resolve hostname %s, using as-is: %s", host, e.getMessage());
            return host;
        }
    }

    private void triggerTeradataExecution(String queryId, ConnectorTableHandle table, String targetIps, DynamicFilter dynamicFilter) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) table;
        String tableName = handle.getSchemaTableName().toString();
        
        // Dynamic Filtering: Wait for the filter to be complete before executing
        // This is critical - we need complete filter values from the build side
        // to avoid partial filter application which would miss rows
        if (!config.isEnableDynamicFiltering()) {
            log.info("Dynamic filtering disabled for query %s", queryId);
        } else if (dynamicFilter != null && !dynamicFilter.getCurrentPredicate().isAll()) {
            // Already have predicates, can proceed
            log.info("Dynamic filter already available for query %s", queryId);
        } else if (dynamicFilter != null && dynamicFilter.isAwaitable()) {
            try {
                log.info("Waiting for dynamic filter to complete for query %s...", queryId);
                // Wait for dynamic filter to complete using configured timeout
                // This allows the build side of the JOIN to complete and collect all values
                long startTime = System.currentTimeMillis();
                long timeout = config.getDynamicFilterTimeout().toMillis();
                
                while (!dynamicFilter.isComplete() && 
                       (System.currentTimeMillis() - startTime) < timeout) {
                    dynamicFilter.isBlocked().get(500, java.util.concurrent.TimeUnit.MILLISECONDS);
                }
                
                if (dynamicFilter.isComplete()) {
                    log.info("Dynamic filter completed for query %s", queryId);
                } else {
                    log.warn("Dynamic filter not complete after timeout (%dms) for query %s, proceeding with current filter", timeout, queryId);
                }
            } catch (Exception e) {
                log.warn("Error waiting for dynamic filter for query %s: %s", queryId, e.getMessage());
            }
        }

        // Build column list for SELECT - use projected columns if available
        // NOTE: The projected columns from applyProjection already contain the correct
        // base column names (extracted from ColumnHandle.getName()), so we use them as-is.
        String columnList;
        if (handle.getProjectedColumns().isPresent() && !handle.getProjectedColumns().get().isEmpty()) {
            columnList = String.join(", ", handle.getProjectedColumns().get());
            log.info("Using column pruning for query %s: %s", queryId, columnList);
        } else {
            columnList = "*";
        }
        
        // Build WHERE clause
        List<String> predicateParts = new ArrayList<>();
        
        // 1. Static predicates from Metadata.applyFilter
        handle.getPredicateClause().ifPresent(predicateParts::add);
        
        // 2. Dynamic predicates from Trino's DynamicFilter
        if (dynamicFilter != null && !dynamicFilter.getCurrentPredicate().isAll()) {
            io.trino.spi.predicate.TupleDomain<ColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate();
            dynamicPredicate.getDomains().ifPresent(domains -> {
                for (java.util.Map.Entry<ColumnHandle, io.trino.spi.predicate.Domain> entry : domains.entrySet()) {
                    TrinoExportColumnHandle column = (TrinoExportColumnHandle) entry.getKey();
                    String sql = TrinoExportFilterUtils.domainToSql(column.getName(), column.getType(), entry.getValue());
                    if (sql != null) {
                        predicateParts.add(sql);
                        log.info("Using dynamic filter for query %s: %s", queryId, sql);
                    }
                }
            });
        }

        String whereClause = predicateParts.isEmpty() ? "" : " WHERE " + String.join(" AND ", predicateParts);
        if (!whereClause.isEmpty()) {
            log.info("Final pushed down WHERE clause for query %s: %s", queryId, whereClause);
        }

        // Build SAMPLE clause from limit pushdown
        String sampleClause = "";
        if (handle.getLimit().isPresent()) {
            sampleClause = " SAMPLE " + handle.getLimit().getAsLong();
            log.info("Using limit pushdown for query %s: %s", queryId, sampleClause);
        }
        
        // Teradata Table Operator syntax with parameters passed via secondary DIMENSION stream
        // This is more robust than USING clause across different Teradata versions.
        String udfDatabase = config.getUdfDatabase();
        String udfName = config.getUdfName();
        String fullUdfName = udfDatabase + "." + udfName;
        String sql = String.format(
            "SELECT * FROM " + fullUdfName + "(" +
            "  ON (SELECT %s FROM %s%s%s) " +
            "  ON (SELECT CAST('%s' AS VARCHAR(2048)) as target_ips, CAST('%s' AS VARCHAR(256)) as qid, CAST('%s' AS VARCHAR(256)) as token, CAST(%d AS INTEGER) as batch_size, CAST(%d AS INTEGER) as compression_enabled) DIMENSION" +
            ") AS export_result", columnList, tableName, whereClause, sampleClause, 
            targetIps, queryId, config.getSecurityToken() != null ? config.getSecurityToken() : "", config.getBatchSize(), config.isCompressionEnabled() ? 1 : 0);

        log.info("Executing Teradata SQL for query %s: %s", queryId, sql);

        try {
            // Explicitly load driver to ensure it's available in this context
            Driver driver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
            Properties props = new Properties();
            props.setProperty("user", config.getTeradataUser());
            props.setProperty("password", config.getTeradataPassword());
            
            try (Connection conn = driver.connect(config.getTeradataUrl(), props);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                log.info("Teradata SQL execution finished successfully for query %s", queryId);
            }
        } catch (Exception e) {
            log.error(e, "Teradata SQL execution failed for query %s", queryId);
        } finally {
            // Signal JDBC completion - EOS will be sent when all connections close
            DataBufferRegistry.signalJdbcFinished(queryId);
        }
    }
}
