package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.NodeManager;
import io.trino.spi.Node;

import javax.inject.Inject;
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
        
        String queryId = session.getQueryId();
        DataBufferRegistry.registerQuery(queryId);

        // For multi-worker clusters: each worker should have its own bridge instance.
        // We pass distinct worker IPs so Teradata AMPs can be distributed across them.
        // In single-worker mode, we fall back to the configured address.
        String allWorkerIps;
        if (workers.size() > 1) {
            // Multi-worker: use actual worker host addresses
            allWorkerIps = workers.stream()
                    .map(node -> node.getHost() + ":" + config.getBridgePort())
                    .distinct()  // Remove duplicates if any
                    .collect(Collectors.joining(","));
        } else {
            // Single-worker: use configured address (handles loopback issues)
            allWorkerIps = config.getTrinoAddress() + ":" + config.getBridgePort();
        }

        log.info("Registering query %s. Worker IPs for AMP distribution: %s", queryId, allWorkerIps);

        List<ConnectorSplit> splits = workers.stream()
                .map(node -> new TrinoExportSplit(node.getHost(), queryId, allWorkerIps))
                .collect(Collectors.toList());

        executor.submit(() -> triggerTeradataExecution(queryId, table, allWorkerIps));

        return new FixedSplitSource(splits);
    }

    private void triggerTeradataExecution(String queryId, ConnectorTableHandle table, String targetIps) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) table;
        String tableName = handle.getSchemaTableName().toString();
        
        // Teradata Table Operator syntax:
        // SELECT * FROM TrinoExport.ExportToTrino(
        //   ON (SELECT * FROM table)
        //   USING TargetIPs('ip:port') QueryID('uuid')
        // ) AS t
        // Teradata Table Operator syntax with parameters passed via secondary DIMENSION stream
        // This is more robust than USING clause across different Teradata versions.
        String sql = String.format(
            "SELECT * FROM TrinoExport.ExportToTrino(" +
            "  ON (SELECT * FROM %s) " +
            "  ON (SELECT '%s' as target_ips, '%s' as qid) DIMENSION" +
            ") AS export_result", tableName, targetIps, queryId);

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
                // Note: Do NOT signal end of stream here. 
                // The data transfer happens asynchronously via the bridge.
                // The Flight server will signal end of stream when it receives all data.
            }
        } catch (Exception e) {
            log.error(e, "Teradata SQL execution failed for query %s", queryId);
        } finally {
            // Signal end of stream once Teradata is done.
            // Small sleep to ensure the last batches from the bridge have reached the Flight server.
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            DataBufferRegistry.signalEndOfStream(queryId);
        }
    }
}
