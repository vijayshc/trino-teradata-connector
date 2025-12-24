import java.sql.*;
import java.util.*;

/**
 * Trino Teradata Export Connector - Comprehensive Java Test Suite
 * 
 * Modular, fast execution using persistent JDBC connection.
 * All test cases from run_connector_tests.sh ported to Java.
 * 
 * Usage:
 *   ./run_java_tests.sh [category]
 * 
 * Categories:
 *   all        - Run all tests (default)
 *   basic      - Basic connectivity (Section 1)
 *   integer    - Integer types (Section 2)
 *   decimal    - Decimal types (Section 3)
 *   float      - Float types (Section 4)
 *   char       - Character types (Section 5)
 *   datetime   - Date/Time types (Section 6)
 *   null       - NULL handling (Section 7)
 *   edge       - Edge cases (Section 8)
 *   join       - JOIN operations (Section 9)
 *   agg        - Aggregations (Section 10)
 *   limit      - LIMIT pushdown (Section 11)
 *   predicate  - Predicate pushdown (Section 12)
 */
public class TrinoConnectorTests {
    
    private static final String JDBC_URL = "jdbc:trino://localhost:8080/tdexport/trinoexport";
    private static final String USER = "vijay";
    
    private static Connection conn;
    private static int total = 0, passed = 0, failed = 0, skipped = 0;
    private static List<String> failures = new ArrayList<>();
    
    // ANSI colors
    private static final String G = "\u001B[32m", R = "\u001B[31m", Y = "\u001B[33m", B = "\u001B[34m", N = "\u001B[0m";
    
    public static void main(String[] args) {
        String cat = args.length > 0 ? args[0].toLowerCase() : "all";
        
        System.out.println("============================================================");
        System.out.println(" Trino Teradata Export Connector - Java Test Suite");
        System.out.println(" " + new java.util.Date());
        System.out.println("============================================================\n");
        
        try {
            System.out.println("Connecting to Trino...");
            conn = DriverManager.getConnection(JDBC_URL, USER, null);
            System.out.println("Connected!\n");
            
            runTests(cat);
            
        } catch (SQLException e) {
            System.err.println("Connection error: " + e.getMessage());
            System.exit(1);
        } finally {
            try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
        }
        
        printSummary();
        System.exit(failed > 0 ? 1 : 0);
    }
    
    private static void runTests(String cat) throws SQLException {
        if (cat.equals("all") || cat.equals("basic"))     testBasic();
        if (cat.equals("all") || cat.equals("integer"))   testIntegerTypes();
        if (cat.equals("all") || cat.equals("decimal"))   testDecimalTypes();
        if (cat.equals("all") || cat.equals("float"))     testFloatTypes();
        if (cat.equals("all") || cat.equals("char"))      testCharTypes();
        if (cat.equals("all") || cat.equals("datetime"))  testDateTimeTypes();
        if (cat.equals("all") || cat.equals("null"))      testNullHandling();
        if (cat.equals("all") || cat.equals("edge"))      testEdgeCases();
        if (cat.equals("all") || cat.equals("join"))      testJoinOperations();
        if (cat.equals("all") || cat.equals("agg"))       testAggregations();
        if (cat.equals("all") || cat.equals("limit"))     testLimitPushdown();
        if (cat.equals("all") || cat.equals("predicate")) testPredicatePushdown();
    }
    
    // ========== Section 1: Basic Connectivity ==========
    private static void testBasic() throws SQLException {
        section("Section 1: Basic Connectivity");
        assertCount("1.1 Integer types table", "test_integer_types", 6);
        assertCount("1.2 Decimal types table", "test_decimal_types", 6);
        assertCount("1.3 Float types table", "test_float_types", 7);
        assertCount("1.4 Char types table", "test_char_types", 5);
        assertCount("1.5 Datetime types table", "test_datetime_types", 5);
        assertCount("1.6 Null handling table", "test_null_handling", 6);
        assertCount("1.7 Edge cases table", "test_edge_cases", 5);
    }
    
    // ========== Section 2: Integer Types ==========
    private static void testIntegerTypes() throws SQLException {
        section("Section 2: Integer Types");
        eq("2.1 BYTEINT zero", "0", q("SELECT col_byteint FROM test_integer_types WHERE test_id = 1"));
        eq("2.2 BYTEINT max", "127", q("SELECT col_byteint FROM test_integer_types WHERE test_id = 4"));
        eq("2.3 BYTEINT min", "-128", q("SELECT col_byteint FROM test_integer_types WHERE test_id = 5"));
        eq("2.4 SMALLINT max", "32767", q("SELECT col_smallint FROM test_integer_types WHERE test_id = 4"));
        eq("2.5 SMALLINT min", "-32768", q("SELECT col_smallint FROM test_integer_types WHERE test_id = 5"));
        eq("2.6 INTEGER max", "2147483647", q("SELECT col_integer FROM test_integer_types WHERE test_id = 4"));
        eq("2.7 INTEGER min", "-2147483648", q("SELECT col_integer FROM test_integer_types WHERE test_id = 5"));
        eq("2.8 BIGINT max", "9223372036854775807", q("SELECT col_bigint FROM test_integer_types WHERE test_id = 4"));
        eq("2.9 BIGINT min", "-9223372036854775808", q("SELECT col_bigint FROM test_integer_types WHERE test_id = 5"));
        eq("2.10 BIGINT large", "9876543210123", q("SELECT col_bigint FROM test_integer_types WHERE test_id = 6"));
    }
    
    // ========== Section 3: Decimal Types ==========
    private static void testDecimalTypes() throws SQLException {
        section("Section 3: Decimal Types");
        eq("3.1 DECIMAL(5,2) zero", "0.00", q("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 1"));
        eq("3.2 DECIMAL(5,2) positive", "123.45", q("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 5"));
        eq("3.3 DECIMAL(5,2) negative", "-456.78", q("SELECT col_dec_5_2 FROM test_decimal_types WHERE test_id = 6"));
        eq("3.4 DECIMAL(10,4)", "1234.5678", q("SELECT col_dec_10_4 FROM test_decimal_types WHERE test_id = 5"));
        eq("3.5 DECIMAL(18,6)", "123456.789012", q("SELECT col_dec_18_6 FROM test_decimal_types WHERE test_id = 5"));
    }
    
    // ========== Section 4: Float Types ==========
    private static void testFloatTypes() throws SQLException {
        section("Section 4: Float Types");
        eq("4.1 FLOAT zero", "0.0", q("SELECT col_float FROM test_float_types WHERE test_id = 1"));
        eq("4.2 FLOAT negative", "-1.0", q("SELECT col_float FROM test_float_types WHERE test_id = 3"));
        contains("4.3 FLOAT Pi", "3.14159", q("SELECT ROUND(col_float, 5) FROM test_float_types WHERE test_id = 4"));
    }
    
    // ========== Section 5: Character Types ==========
    private static void testCharTypes() throws SQLException {
        section("Section 5: Character Types");
        eq("5.1 VARCHAR basic", "Hello World", q("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 1"));
        eq("5.2 VARCHAR empty", "", q("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 2"));
        contains("5.3 VARCHAR special", "ABC123", q("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 3"));
        contains("5.4 VARCHAR quote", "It's a test", q("SELECT col_varchar_50 FROM test_char_types WHERE test_id = 4"));
        eq("5.5 CHAR trimmed", "Hello", q("SELECT TRIM(col_char_10) FROM test_char_types WHERE test_id = 1"));
    }
    
    // ========== Section 6: Date/Time Types ==========
    private static void testDateTimeTypes() throws SQLException {
        section("Section 6: Date/Time Types");
        eq("6.1 DATE New Year", "2024-01-01", q("SELECT col_date FROM test_datetime_types WHERE test_id = 1"));
        eq("6.2 DATE End of Year", "2024-12-31", q("SELECT col_date FROM test_datetime_types WHERE test_id = 2"));
        eq("6.3 DATE Leap Year", "2000-02-29", q("SELECT col_date FROM test_datetime_types WHERE test_id = 3"));
        eq("6.4 DATE Unix Epoch", "1970-01-01", q("SELECT col_date FROM test_datetime_types WHERE test_id = 4"));
        contains("6.5 TIME format", ":", q("SELECT CAST(col_time AS VARCHAR) FROM test_datetime_types WHERE test_id = 2"));
    }
    
    // ========== Section 7: NULL Handling ==========
    private static void testNullHandling() throws SQLException {
        section("Section 7: NULL Handling");
        eq("7.1 COUNT(*) with NULLs", "6", q("SELECT COUNT(*) FROM test_null_handling"));
        eq("7.2 COUNT(col_int)", "4", q("SELECT COUNT(col_int) FROM test_null_handling"));
        eq("7.3 COUNT(col_decimal)", "4", q("SELECT COUNT(col_decimal) FROM test_null_handling"));
        eq("7.4 COUNT(col_varchar)", "4", q("SELECT COUNT(col_varchar) FROM test_null_handling"));
        eq("7.5 COUNT(col_date)", "4", q("SELECT COUNT(col_date) FROM test_null_handling"));
        isNull("7.6 NULL filter returns null", q("SELECT col_int FROM test_null_handling WHERE col_int IS NULL LIMIT 1"));
    }
    
    // ========== Section 8: Edge Cases ==========
    private static void testEdgeCases() throws SQLException {
        section("Section 8: Edge Cases");
        eq("8.1 Small positive decimal", "0.01", q("SELECT col_dec FROM test_edge_cases WHERE test_id = 1"));
        eq("8.2 Zero decimal", "0.00", q("SELECT col_dec FROM test_edge_cases WHERE test_id = 2"));
        eq("8.3 Small negative decimal", "-0.01", q("SELECT col_dec FROM test_edge_cases WHERE test_id = 3"));
        eq("8.4 Max precision decimal", "99999.99", q("SELECT col_dec FROM test_edge_cases WHERE test_id = 4"));
        eq("8.5 Min precision decimal", "-99999.99", q("SELECT col_dec FROM test_edge_cases WHERE test_id = 5"));
    }
    
    // ========== Section 9: JOIN Operations ==========
    private static void testJoinOperations() throws SQLException {
        section("Section 9: JOIN Operations");
        assertCount("9.1 Dim table count", "test_join_dim", 5);
        assertCount("9.2 Fact table count", "test_join_fact", 8);
        eq("9.3 Simple JOIN count", "8", q("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id"));
        eq("9.4 JOIN with filter", "5", q("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type1'"));
        eq("9.5 JOIN on specific dim", "3", q("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_name = 'Category A'"));
        eq("9.6 JOIN SUM", "550.50", q("SELECT CAST(SUM(f.fact_value) AS VARCHAR) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type2'"));
    }
    
    // ========== Section 10: Aggregations ==========
    private static void testAggregations() throws SQLException {
        section("Section 10: Aggregations");
        eq("10.1 SUM of integers", "0", q("SELECT SUM(col_integer) FROM test_integer_types WHERE test_id IN (1,2,3)"));
        eq("10.2 AVG of decimals", "222.22", q("SELECT CAST(AVG(col_dec_5_2) AS DECIMAL(10,2)) FROM test_decimal_types WHERE test_id IN (4,5,6)"));
        eq("10.3 MIN of integers", "-2147483648", q("SELECT MIN(col_integer) FROM test_integer_types"));
        eq("10.4 MAX of integers", "2147483647", q("SELECT MAX(col_integer) FROM test_integer_types"));
        eq("10.5 GROUP BY count", "2", q("SELECT COUNT(*) FROM test_join_dim GROUP BY dim_category ORDER BY COUNT(*) DESC LIMIT 1"));
    }
    
    // ========== Section 11: LIMIT Pushdown ==========
    private static void testLimitPushdown() throws SQLException {
        section("Section 11: LIMIT Pushdown");
        eq("11.1 LIMIT 1", "1", q("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types LIMIT 1)"));
        eq("11.2 LIMIT 3", "3", q("SELECT COUNT(*) FROM (SELECT * FROM test_integer_types LIMIT 3)"));
    }
    
    // ========== Section 12: Predicate Pushdown ==========
    private static void testPredicatePushdown() throws SQLException {
        section("Section 12: Predicate Pushdown");
        eq("12.1 Equality filter", "42", q("SELECT col_byteint FROM test_integer_types WHERE test_id = 6"));
        eq("12.2 IN filter", "3", q("SELECT COUNT(*) FROM test_integer_types WHERE test_id IN (1, 2, 3)"));
        eq("12.3 Range filter", "2", q("SELECT COUNT(*) FROM test_edge_cases WHERE col_dec > 0"));
        eq("12.4 String filter", "1", q("SELECT test_id FROM test_char_types WHERE col_varchar_50 = 'Hello World'"));
    }
    
    // ========== Helper Methods ==========
    private static String q(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }
    
    private static void section(String name) { 
        System.out.println("\n" + B + "=== " + name + " ===" + N); 
    }
    
    private static void eq(String name, String expected, String actual) {
        total++;
        String e = expected != null ? expected.trim() : null;
        String a = actual != null ? actual.trim() : null;
        if (Objects.equals(e, a)) {
            System.out.println(G + "[PASS]" + N + " " + name);
            passed++;
        } else {
            System.out.println(R + "[FAIL]" + N + " " + name + " | Expected: " + Y + expected + N + " | Got: " + Y + actual + N);
            failed++;
            failures.add(name);
        }
    }
    
    private static void contains(String name, String sub, String actual) {
        total++;
        if (actual != null && actual.contains(sub)) {
            System.out.println(G + "[PASS]" + N + " " + name);
            passed++;
        } else {
            System.out.println(R + "[FAIL]" + N + " " + name + " | Expected to contain: " + Y + sub + N + " | Got: " + Y + actual + N);
            failed++;
            failures.add(name);
        }
    }
    
    private static void isNull(String name, String actual) {
        total++;
        if (actual == null) {
            System.out.println(G + "[PASS]" + N + " " + name);
            passed++;
        } else {
            System.out.println(R + "[FAIL]" + N + " " + name + " | Expected null | Got: " + Y + actual + N);
            failed++;
            failures.add(name);
        }
    }
    
    private static void assertCount(String name, String table, int expected) throws SQLException {
        eq(name, String.valueOf(expected), q("SELECT COUNT(*) FROM " + table));
    }
    
    private static void printSummary() {
        System.out.println("\n============================================================");
        if (failed == 0) {
            System.out.println(G + "ALL TESTS PASSED: " + passed + "/" + total + N);
        } else {
            System.out.println(R + "FAILED: " + failed + " | PASSED: " + passed + " | TOTAL: " + total + N);
            System.out.println("\nFailed tests:");
            failures.forEach(f -> System.out.println("  - " + f));
        }
        System.out.println("============================================================");
    }
}
