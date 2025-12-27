package io.trino.stress;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Component-Level Bottleneck Analyzer.
 * 
 * Runs targeted tests to isolate specific bottlenecks:
 * 1. Connection Pool Test - Tests JDBC connection overhead
 * 2. Thread Pool Test - Tests concurrent query handling
 * 3. Buffer Queue Test - Tests data buffer throughput
 * 4. Serialization Test - Tests decompression/parsing overhead
 * 
 * Usage:
 *   java -cp ... io.trino.stress.BottleneckAnalyzer [test-name]
 * 
 * Test Names:
 *   connection  - Test connection pool limits
 *   threadpool  - Test thread pool saturation
 *   buffer      - Test buffer queue backpressure  
 *   serialize   - Test data parsing overhead
 *   all         - Run all tests (default)
 */
public class BottleneckAnalyzer {
    
    private static final String JDBC_URL = "jdbc:trino://localhost:8080/tdexport";
    
    public static void main(String[] args) {
        String testName = args.length > 0 ? args[0] : "all";
        
        BottleneckAnalyzer analyzer = new BottleneckAnalyzer();
        
        System.out.println("=".repeat(70));
        System.out.println("  BOTTLENECK ANALYZER - Component Level Stress Testing");
        System.out.println("=".repeat(70));
        
        switch (testName.toLowerCase()) {
            case "connection" -> analyzer.testConnectionPool();
            case "threadpool" -> analyzer.testThreadPoolSaturation();
            case "buffer" -> analyzer.testBufferQueuePressure();
            case "serialize" -> analyzer.testSerializationOverhead();
            case "escalation" -> analyzer.testLoadEscalation();
            default -> {
                analyzer.testConnectionPool();
                analyzer.testThreadPoolSaturation();
                analyzer.testBufferQueuePressure();
                analyzer.testSerializationOverhead();
                analyzer.testLoadEscalation();
            }
        }
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  ANALYSIS COMPLETE");
        System.out.println("=".repeat(70));
    }
    
    /**
     * Test 1: Connection Pool Limits
     * Creates many simultaneous connections to test JDBC pool exhaustion
     */
    private void testConnectionPool() {
        System.out.println("\n--- TEST 1: CONNECTION POOL LIMITS ---");
        
        int[] connectionCounts = {5, 10, 20, 50, 100};
        
        for (int count : connectionCounts) {
            List<Connection> connections = new ArrayList<>();
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            
            Instant start = Instant.now();
            ExecutorService executor = Executors.newFixedThreadPool(count);
            CountDownLatch latch = new CountDownLatch(count);
            
            for (int i = 0; i < count; i++) {
                executor.submit(() -> {
                    try {
                        Connection conn = DriverManager.getConnection(JDBC_URL, "vijay", null);
                        synchronized (connections) {
                            connections.add(conn);
                        }
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            try {
                boolean completed = latch.await(30, TimeUnit.SECONDS);
                Instant end = Instant.now();
                long durationMs = Duration.between(start, end).toMillis();
                
                System.out.printf("  %3d connections: %d succeeded, %d failed, %d ms%s%n",
                        count, successCount.get(), failCount.get(), durationMs,
                        completed ? "" : " (TIMEOUT)");
                
                if (failCount.get() > 0) {
                    System.out.printf("      ⚠️  Connection failures detected at %d connections%n", count);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Cleanup
            for (Connection conn : connections) {
                try { conn.close(); } catch (Exception ignored) {}
            }
            executor.shutdownNow();
        }
    }
    
    /**
     * Test 2: Thread Pool Saturation
     * Bombards with queries to identify when thread pools become saturated
     */
    private void testThreadPoolSaturation() {
        System.out.println("\n--- TEST 2: THREAD POOL SATURATION ---");
        
        int[] concurrencyLevels = {5, 10, 25, 50, 100};
        String query = "SELECT * FROM dbc.databases LIMIT 1";
        
        for (int concurrency : concurrencyLevels) {
            AtomicLong totalTime = new AtomicLong(0);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            ConcurrentLinkedQueue<Long> responseTimes = new ConcurrentLinkedQueue<>();
            
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            int queriesPerThread = 10;
            CountDownLatch latch = new CountDownLatch(concurrency * queriesPerThread);
            
            Instant start = Instant.now();
            
            for (int i = 0; i < concurrency; i++) {
                executor.submit(() -> {
                    for (int j = 0; j < queriesPerThread; j++) {
                        try {
                            Instant qStart = Instant.now();
                            executeQuery(query);
                            Instant qEnd = Instant.now();
                            long duration = Duration.between(qStart, qEnd).toMillis();
                            responseTimes.add(duration);
                            totalTime.addAndGet(duration);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            
            try {
                boolean completed = latch.await(120, TimeUnit.SECONDS);
                Instant end = Instant.now();
                long wallClockMs = Duration.between(start, end).toMillis();
                
                List<Long> times = new ArrayList<>(responseTimes);
                Collections.sort(times);
                long p50 = times.isEmpty() ? 0 : times.get(times.size() / 2);
                long p99 = times.isEmpty() ? 0 : times.get((int)(times.size() * 0.99));
                
                System.out.printf("  %3d threads: %d ok, %d fail | P50: %d ms, P99: %d ms | Total: %d ms%s%n",
                        concurrency, successCount.get(), failCount.get(), 
                        p50, p99, wallClockMs,
                        completed ? "" : " (TIMEOUT)");
                
                if (p99 > p50 * 3) {
                    System.out.printf("      ⚠️  High tail latency at %d threads (thread contention)%n", concurrency);
                }
                if (failCount.get() > 0) {
                    System.out.printf("      ⚠️  Failures at %d threads (pool exhaustion)%n", concurrency);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            executor.shutdownNow();
        }
    }
    
    /**
     * Test 3: Buffer Queue Backpressure
     * Tests data throughput with varying data sizes
     */
    private void testBufferQueuePressure() {
        System.out.println("\n--- TEST 3: BUFFER QUEUE PRESSURE (Data Volume) ---");
        
        // Queries with progressively larger result sets
        Map<String, String> testQueries = new LinkedHashMap<>();
        testQueries.put("1 row", "SELECT * FROM dbc.databases LIMIT 1");
        testQueries.put("10 rows", "SELECT * FROM dbc.tables LIMIT 10");
        testQueries.put("100 rows", "SELECT * FROM dbc.tables LIMIT 100");
        testQueries.put("1000 rows", "SELECT * FROM dbc.columns LIMIT 1000");
        
        int concurrency = 20;
        int queriesEach = 5;
        
        for (Map.Entry<String, String> entry : testQueries.entrySet()) {
            String label = entry.getKey();
            String query = entry.getValue();
            
            AtomicLong totalRows = new AtomicLong(0);
            AtomicLong totalTimeMs = new AtomicLong(0);
            AtomicInteger failCount = new AtomicInteger(0);
            
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch latch = new CountDownLatch(concurrency * queriesEach);
            
            Instant start = Instant.now();
            
            for (int i = 0; i < concurrency; i++) {
                executor.submit(() -> {
                    for (int j = 0; j < queriesEach; j++) {
                        try {
                            Instant qStart = Instant.now();
                            int rows = executeQueryAndCount(query);
                            Instant qEnd = Instant.now();
                            totalRows.addAndGet(rows);
                            totalTimeMs.addAndGet(Duration.between(qStart, qEnd).toMillis());
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            
            try {
                boolean completed = latch.await(120, TimeUnit.SECONDS);
                Instant end = Instant.now();
                long wallClockMs = Duration.between(start, end).toMillis();
                long rows = totalRows.get();
                double rowsPerSec = wallClockMs > 0 ? (rows * 1000.0 / wallClockMs) : 0;
                
                System.out.printf("  %-12s: %,8d rows | %.1f rows/sec | %d failures%s%n",
                        label, rows, rowsPerSec, failCount.get(),
                        completed ? "" : " (TIMEOUT)");
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            executor.shutdownNow();
        }
    }
    
    /**
     * Test 4: Serialization/Decompression Overhead
     * Tests CPU-bound parsing operations
     */
    private void testSerializationOverhead() {
        System.out.println("\n--- TEST 4: SERIALIZATION/DECOMPRESSION OVERHEAD ---");
        
        // Test different data types that require different parsing
        Map<String, String> typeQueries = new LinkedHashMap<>();
        typeQueries.put("Integers", "SELECT * FROM dbc.tables WHERE tablekind = 'T' LIMIT 20");
        typeQueries.put("Mixed", "SELECT databasename, tablename, tablekind, commentstring FROM dbc.tables LIMIT 20");
        typeQueries.put("Strings", "SELECT databasename, tablename, commentstring FROM dbc.tables LIMIT 20");
        typeQueries.put("Large", "SELECT * FROM dbc.columns LIMIT 50");
        
        int iterations = 10;
        
        for (Map.Entry<String, String> entry : typeQueries.entrySet()) {
            String label = entry.getKey();
            String query = entry.getValue();
            
            List<Long> times = new ArrayList<>();
            int totalRows = 0;
            int failures = 0;
            
            for (int i = 0; i < iterations; i++) {
                try {
                    Instant start = Instant.now();
                    int rows = executeQueryAndCount(query);
                    Instant end = Instant.now();
                    times.add(Duration.between(start, end).toMillis());
                    totalRows += rows;
                } catch (Exception e) {
                    failures++;
                }
            }
            
            if (!times.isEmpty()) {
                Collections.sort(times);
                long median = times.get(times.size() / 2);
                long min = times.get(0);
                long max = times.get(times.size() - 1);
                
                System.out.printf("  %-12s: Median %d ms, Min %d ms, Max %d ms (rows: %d, failures: %d)%n",
                        label, median, min, max, totalRows / iterations, failures);
                
                if (max > median * 2) {
                    System.out.printf("      ⚠️  High variance for %s (possible GC or contention)%n", label);
                }
            }
        }
    }
    
    /**
     * Test 5: Load Escalation Test
     * Gradually increases load to find breaking point
     */
    private void testLoadEscalation() {
        System.out.println("\n--- TEST 5: LOAD ESCALATION (Finding Breaking Point) ---");
        
        String query = "SELECT * FROM dbc.tables LIMIT 10";
        
        System.out.println("  Gradually increasing load until failure or timeout...");
        System.out.printf("  %-12s %-12s %-12s %-12s %-15s%n", 
                "Concurrency", "Success", "Failed", "Avg(ms)", "Status");
        
        for (int concurrency = 10; concurrency <= 200; concurrency += 20) {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            AtomicLong totalTime = new AtomicLong(0);
            
            int queriesTotal = concurrency * 3;  // 3 queries each
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch latch = new CountDownLatch(queriesTotal);
            
            Instant start = Instant.now();
            
            for (int i = 0; i < queriesTotal; i++) {
                executor.submit(() -> {
                    try {
                        Instant qStart = Instant.now();
                        executeQuery(query);
                        Instant qEnd = Instant.now();
                        totalTime.addAndGet(Duration.between(qStart, qEnd).toMillis());
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            String status;
            try {
                boolean completed = latch.await(60, TimeUnit.SECONDS);
                long avgMs = successCount.get() > 0 ? totalTime.get() / successCount.get() : 0;
                
                if (!completed) {
                    status = "⚠️  TIMEOUT";
                } else if (failCount.get() > queriesTotal * 0.1) {
                    status = "⚠️  HIGH FAILURES";
                } else if (avgMs > 5000) {
                    status = "⚠️  SLOW";
                } else {
                    status = "✅ OK";
                }
                
                System.out.printf("  %-12d %-12d %-12d %-12d %-15s%n",
                        concurrency, successCount.get(), failCount.get(), avgMs, status);
                
                if (status.startsWith("⚠️")) {
                    System.out.println("\n  Breaking point identified at concurrency: " + concurrency);
                    executor.shutdownNow();
                    break;
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            
            executor.shutdownNow();
        }
    }
    
    private void executeQuery(String query) throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, "vijay", null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                // Consume rows
            }
        }
    }
    
    private int executeQueryAndCount(String query) throws SQLException {
        int count = 0;
        try (Connection conn = DriverManager.getConnection(JDBC_URL, "vijay", null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }
}
