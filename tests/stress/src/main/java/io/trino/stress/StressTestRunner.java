package io.trino.stress;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Lightweight Stress Test Runner for Teradata-Trino Connector.
 * 
 * Design goals:
 * 1. Identify concurrency bottlenecks
 * 2. Measure response time degradation under load
 * 3. Detect deadlocks and thread starvation
 * 4. Monitor memory pressure
 * 
 * Usage:
 *   java -cp target/classes:/path/to/trino-jdbc.jar io.trino.stress.StressTestRunner [options]
 * 
 * Options:
 *   --concurrency N      Number of concurrent queries (default: 10)
 *   --duration N         Test duration in seconds (default: 60)
 *   --ramp-up N          Ramp-up period in seconds (default: 5)
 *   --query-type TYPE    Query type: small|medium|large|mixed (default: mixed)
 *   --url URL            Trino JDBC URL (default: jdbc:trino://localhost:8080/tdexport)
 */
public class StressTestRunner {
    
    private static final String DEFAULT_URL = "jdbc:trino://localhost:8080/tdexport";
    private static final int DEFAULT_CONCURRENCY = 10;
    private static final int DEFAULT_DURATION_SECONDS = 60;
    private static final int DEFAULT_RAMP_UP_SECONDS = 5;
    
    // Metrics
    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong successfulQueries = new AtomicLong(0);
    private final AtomicLong failedQueries = new AtomicLong(0);
    private final AtomicLong totalRows = new AtomicLong(0);
    private final AtomicLong totalExecutionTimeNanos = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> responseTimes = new ConcurrentLinkedQueue<>();
    
    // Active query tracking for deadlock detection
    private final ConcurrentHashMap<Long, QueryInfo> activeQueries = new ConcurrentHashMap<>();
    
    // Test queries - categorized by expected data volume (using dbc.* system tables)
    private final List<String> smallQueries = Arrays.asList(
            "SELECT 1",
            "SELECT COUNT(*) FROM dbc.tables",
            "SELECT * FROM dbc.databases LIMIT 1",
            "SELECT databasename, tablename FROM dbc.tables LIMIT 5"
    );
    
    private final List<String> mediumQueries = Arrays.asList(
            "SELECT * FROM dbc.tables LIMIT 50",
            "SELECT * FROM dbc.columns LIMIT 50",
            "SELECT * FROM dbc.databases",
            "SELECT databasename, tablename, tablekind FROM dbc.tables LIMIT 100"
    );
    
    private final List<String> largeQueries = Arrays.asList(
            "SELECT * FROM dbc.tables LIMIT 500",
            "SELECT * FROM dbc.columns LIMIT 500",
            "SELECT * FROM dbc.tables WHERE databasename = 'DBC'"
    );
    
    private volatile boolean running = true;
    private String jdbcUrl;
    private int concurrency;
    private int durationSeconds;
    private int rampUpSeconds;
    private String queryType;
    
    public static void main(String[] args) {
        StressTestRunner runner = new StressTestRunner();
        runner.parseArgs(args);
        runner.run();
    }
    
    private void parseArgs(String[] args) {
        this.jdbcUrl = DEFAULT_URL;
        this.concurrency = DEFAULT_CONCURRENCY;
        this.durationSeconds = DEFAULT_DURATION_SECONDS;
        this.rampUpSeconds = DEFAULT_RAMP_UP_SECONDS;
        this.queryType = "mixed";
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--concurrency" -> concurrency = Integer.parseInt(args[++i]);
                case "--duration" -> durationSeconds = Integer.parseInt(args[++i]);
                case "--ramp-up" -> rampUpSeconds = Integer.parseInt(args[++i]);
                case "--query-type" -> queryType = args[++i];
                case "--url" -> jdbcUrl = args[++i];
                case "--help" -> {
                    printHelp();
                    System.exit(0);
                }
            }
        }
    }
    
    private void printHelp() {
        System.out.println("""
            Teradata-Trino Stress Test Runner
            
            Usage: java -jar stress-test.jar [options]
            
            Options:
              --concurrency N      Number of concurrent queries (default: 10)
              --duration N         Test duration in seconds (default: 60)
              --ramp-up N          Ramp-up period in seconds (default: 5)
              --query-type TYPE    Query type: small|medium|large|mixed (default: mixed)
              --url URL            Trino JDBC URL (default: jdbc:trino://localhost:8080/tdexport)
              --help               Show this help
            
            Query Types:
              small   - Simple queries returning few rows (COUNT, LIMIT 1)
              medium  - Moderate queries returning ~100 rows
              large   - Heavy queries with JOINs or full table scans
              mixed   - Random mix of all query types
            """);
    }
    
    public void run() {
        System.out.println("=".repeat(70));
        System.out.println("  TERADATA-TRINO CONNECTOR STRESS TEST");
        System.out.println("=".repeat(70));
        System.out.printf("  Concurrency:    %d threads%n", concurrency);
        System.out.printf("  Duration:       %d seconds%n", durationSeconds);
        System.out.printf("  Ramp-up:        %d seconds%n", rampUpSeconds);
        System.out.printf("  Query Type:     %s%n", queryType);
        System.out.printf("  JDBC URL:       %s%n", jdbcUrl);
        System.out.println("=".repeat(70));
        
        // Validate connectivity first
        if (!validateConnectivity()) {
            System.err.println("FATAL: Cannot connect to Trino. Aborting stress test.");
            System.exit(1);
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(concurrency + 2);
        Instant startTime = Instant.now();
        
        // Start monitor thread
        executor.submit(() -> monitorThread(startTime));
        
        // Start deadlock detector
        executor.submit(() -> deadlockDetectorThread());
        
        // Ramp up workers gradually
        for (int i = 0; i < concurrency; i++) {
            final int workerId = i;
            executor.submit(() -> workerThread(workerId, startTime));
            
            if (rampUpSeconds > 0 && i < concurrency - 1) {
                try {
                    Thread.sleep((rampUpSeconds * 1000L) / concurrency);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        // Wait for test duration
        try {
            Thread.sleep(durationSeconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        running = false;
        
        // Allow graceful shutdown
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.out.println("WARNING: Some threads did not terminate gracefully");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        
        printFinalReport(startTime);
    }
    
    private boolean validateConnectivity() {
        System.out.print("Validating connectivity... ");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "vijay", null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next()) {
                System.out.println("OK");
                return true;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
        }
        return false;
    }
    
    private void workerThread(int workerId, Instant testStart) {
        Random random = new Random(workerId);
        List<String> queries = selectQuerySet();
        
        while (running) {
            String query = queries.get(random.nextInt(queries.size()));
            long queryId = Thread.currentThread().getId();
            
            QueryInfo info = new QueryInfo(query, System.currentTimeMillis(), workerId);
            activeQueries.put(queryId, info);
            
            try {
                Instant start = Instant.now();
                int rows = executeQuery(query);
                Instant end = Instant.now();
                
                long durationMs = Duration.between(start, end).toMillis();
                responseTimes.add(durationMs);
                totalExecutionTimeNanos.addAndGet(Duration.between(start, end).toNanos());
                totalRows.addAndGet(rows);
                successfulQueries.incrementAndGet();
                
            } catch (Exception e) {
                failedQueries.incrementAndGet();
                String errorType = categorizeError(e);
                errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
                
            } finally {
                totalQueries.incrementAndGet();
                activeQueries.remove(queryId);
            }
            
            // Small delay to prevent CPU spin
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private List<String> selectQuerySet() {
        return switch (queryType.toLowerCase()) {
            case "small" -> smallQueries;
            case "medium" -> mediumQueries;
            case "large" -> largeQueries;
            default -> {
                List<String> all = new ArrayList<>();
                all.addAll(smallQueries);
                all.addAll(mediumQueries);
                all.addAll(largeQueries);
                yield all;
            }
        };
    }
    
    private int executeQuery(String query) throws SQLException {
        int rows = 0;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "vijay", null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                rows++;
            }
        }
        return rows;
    }
    
    private String categorizeError(Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        
        if (msg.contains("timeout") || msg.contains("Timeout")) return "TIMEOUT";
        if (msg.contains("connection") || msg.contains("Connection")) return "CONNECTION";
        if (msg.contains("memory") || msg.contains("Memory") || msg.contains("OOM")) return "MEMORY";
        if (msg.contains("thread") || msg.contains("Thread")) return "THREAD_POOL";
        if (msg.contains("queue") || msg.contains("Queue")) return "QUEUE_FULL";
        if (msg.contains("socket") || msg.contains("Socket")) return "SOCKET";
        if (msg.contains("rejected") || msg.contains("Rejected")) return "REJECTED";
        
        return "OTHER: " + msg.substring(0, Math.min(50, msg.length()));
    }
    
    private void monitorThread(Instant testStart) {
        Runtime runtime = Runtime.getRuntime();
        
        System.out.println("\n--- LIVE METRICS ---");
        System.out.printf("%-8s %8s %8s %8s %10s %10s %8s%n", 
                "Time", "Total", "Success", "Failed", "Rows", "Avg(ms)", "Memory");
        
        while (running) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
            
            long elapsed = Duration.between(testStart, Instant.now()).getSeconds();
            long total = totalQueries.get();
            long success = successfulQueries.get();
            long failed = failedQueries.get();
            long rows = totalRows.get();
            long avgMs = total > 0 ? totalExecutionTimeNanos.get() / total / 1_000_000 : 0;
            long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            
            System.out.printf("%-8s %8d %8d %8d %10d %10d %7dMB%n",
                    formatTime(elapsed), total, success, failed, rows, avgMs, usedMb);
        }
    }
    
    private void deadlockDetectorThread() {
        while (running) {
            try {
                Thread.sleep(10000);  // Check every 10 seconds
            } catch (InterruptedException e) {
                break;
            }
            
            long now = System.currentTimeMillis();
            List<QueryInfo> stuckQueries = new ArrayList<>();
            
            for (Map.Entry<Long, QueryInfo> entry : activeQueries.entrySet()) {
                QueryInfo info = entry.getValue();
                long runningMs = now - info.startTime;
                
                // Alert if query running > 30 seconds
                if (runningMs > 30000) {
                    stuckQueries.add(info);
                }
            }
            
            if (!stuckQueries.isEmpty()) {
                System.out.println("\n!!! POTENTIAL BOTTLENECK DETECTED !!!");
                System.out.printf("  %d queries stuck for >30 seconds:%n", stuckQueries.size());
                for (QueryInfo info : stuckQueries) {
                    System.out.printf("    Worker %d: %s (running %d ms)%n", 
                            info.workerId, info.query.substring(0, Math.min(40, info.query.length())),
                            now - info.startTime);
                }
            }
        }
    }
    
    private void printFinalReport(Instant testStart) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  STRESS TEST FINAL REPORT");
        System.out.println("=".repeat(70));
        
        long durationSec = Duration.between(testStart, Instant.now()).getSeconds();
        long total = totalQueries.get();
        long success = successfulQueries.get();
        long failed = failedQueries.get();
        long rows = totalRows.get();
        
        System.out.printf("  Duration:           %s%n", formatTime(durationSec));
        System.out.printf("  Total Queries:      %,d%n", total);
        System.out.printf("  Successful:         %,d (%.1f%%)%n", success, total > 0 ? 100.0 * success / total : 0);
        System.out.printf("  Failed:             %,d (%.1f%%)%n", failed, total > 0 ? 100.0 * failed / total : 0);
        System.out.printf("  Total Rows:         %,d%n", rows);
        System.out.printf("  Throughput:         %.2f queries/sec%n", durationSec > 0 ? (double) total / durationSec : 0);
        
        // Response time percentiles
        List<Long> times = new ArrayList<>(responseTimes);
        if (!times.isEmpty()) {
            Collections.sort(times);
            System.out.println("\n  Response Time Percentiles:");
            System.out.printf("    P50:  %,d ms%n", percentile(times, 50));
            System.out.printf("    P90:  %,d ms%n", percentile(times, 90));
            System.out.printf("    P95:  %,d ms%n", percentile(times, 95));
            System.out.printf("    P99:  %,d ms%n", percentile(times, 99));
            System.out.printf("    Max:  %,d ms%n", times.get(times.size() - 1));
        }
        
        // Error breakdown
        if (!errorCounts.isEmpty()) {
            System.out.println("\n  Error Breakdown:");
            errorCounts.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                    .forEach(e -> System.out.printf("    %-20s: %,d%n", e.getKey(), e.getValue().get()));
        }
        
        // Bottleneck analysis
        System.out.println("\n  BOTTLENECK ANALYSIS:");
        analyzeBottlenecks(times);
        
        System.out.println("=".repeat(70));
    }
    
    private void analyzeBottlenecks(List<Long> times) {
        if (times.isEmpty()) {
            System.out.println("    No data to analyze.");
            return;
        }
        
        long p50 = percentile(times, 50);
        long p99 = percentile(times, 99);
        double failRate = totalQueries.get() > 0 ? 100.0 * failedQueries.get() / totalQueries.get() : 0;
        
        // High tail latency (thread pool saturation)
        if (p99 > p50 * 5) {
            System.out.println("    ⚠️  HIGH TAIL LATENCY: P99 is 5x+ P50");
            System.out.println("       → Indicates thread pool saturation or lock contention");
            System.out.println("       → Consider: Increase max-bridge-threads, max-query-concurrency");
        }
        
        // High failure rate
        if (failRate > 5) {
            System.out.println("    ⚠️  HIGH FAILURE RATE: " + String.format("%.1f%%", failRate));
            if (errorCounts.containsKey("TIMEOUT")) {
                System.out.println("       → Timeouts detected - increase query timeout or reduce concurrency");
            }
            if (errorCounts.containsKey("THREAD_POOL") || errorCounts.containsKey("REJECTED")) {
                System.out.println("       → Thread pool exhaustion - increase max-bridge-threads");
            }
            if (errorCounts.containsKey("QUEUE_FULL")) {
                System.out.println("       → Queue full - increase bridge-queue-capacity");
            }
            if (errorCounts.containsKey("MEMORY")) {
                System.out.println("       → Memory pressure - increase JVM heap or reduce buffer sizes");
            }
        }
        
        // Low throughput
        long throughput = durationSeconds > 0 ? totalQueries.get() / durationSeconds : 0;
        if (throughput < concurrency / 2) {
            System.out.println("    ⚠️  LOW THROUGHPUT: " + throughput + " qps (expected ~" + concurrency + ")");
            System.out.println("       → System is bottlenecked somewhere");
            System.out.println("       → Check Trino server.log for blocking operations");
        }
        
        if (p99 < 1000 && failRate < 1 && throughput >= concurrency / 2) {
            System.out.println("    ✅ HEALTHY: System handling load well");
        }
    }
    
    private long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
    
    private String formatTime(long seconds) {
        return String.format("%02d:%02d", seconds / 60, seconds % 60);
    }
    
    // Inner class for tracking active queries
    private static class QueryInfo {
        final String query;
        final long startTime;
        final int workerId;
        
        QueryInfo(String query, long startTime, int workerId) {
            this.query = query;
            this.startTime = startTime;
            this.workerId = workerId;
        }
    }
}
