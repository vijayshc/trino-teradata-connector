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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class TrinoExportSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(TrinoExportSplitManager.class);
    
    // Thread pool limits for Teradata execution threads
    private static final int CORE_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 50;  // Max concurrent Teradata queries
    private static final int QUEUE_CAPACITY = 100; // Backlog before rejecting
    
    private final NodeManager nodeManager;
    private final TrinoExportConfig config;
    private final ExecutorService executor;

    @Inject
    public TrinoExportSplitManager(NodeManager nodeManager, TrinoExportConfig config) {
        this.nodeManager = nodeManager;
        this.config = config;
        
        // Use bounded thread pool to prevent memory exhaustion from unbounded threads
        this.executor = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                60L, java.util.concurrent.TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "teradata-split-executor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()  // Back-pressure
        );
        
        log.info("TrinoExportSplitManager initialized with maxThreads=%d", MAX_POOL_SIZE);
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

}
