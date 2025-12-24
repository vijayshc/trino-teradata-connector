package io.trino.tests.tdexport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTdExportTest {
    protected static final Logger log = LoggerFactory.getLogger(BaseTdExportTest.class);
    protected static Connection connection;
    
    protected static final String TRINO_URL = "jdbc:trino://localhost:8080/tdexport/trinoexport";
    protected static final String TRINO_USER = "vijay";
    
    @BeforeAll
    public static void setup() throws SQLException {
        log.info("Connecting to Trino at {}", TRINO_URL);
        connection = DriverManager.getConnection(TRINO_URL, TRINO_USER, null);
    }

    @AfterAll
    public static void teardown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    public void logTestStart(TestInfo testInfo) {
        log.info("Running: {}", testInfo.getDisplayName());
    }

    protected void assertQuery(String sql, Object expected) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).as("No data returned for query: " + sql).isTrue();
            Object actual = rs.getObject(1);
            
            String actualStr = actual != null ? actual.toString().trim() : "";
            String expectedStr = expected != null ? expected.toString().trim() : "";
            
            assertThat(actualStr).as("Mismatch for query: " + sql).isEqualTo(expectedStr);
        } catch (SQLException e) {
            throw new RuntimeException("Query failed: " + sql, e);
        }
    }

    protected void assertQueryContains(String sql, String expectedSubstring) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            String actual = rs.getString(1);
            assertThat(actual).as("Query result does not contain: " + expectedSubstring)
                    .contains(expectedSubstring);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<String> getQueryResult(String sql) {
        List<String> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }
    protected void assertLogContains(String... expectedSubstrings) {
        try {
            // Give Trino/Connector a moment to flush logs
            Thread.sleep(1500);
            
            String logPath = "/home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log";
            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(logPath));
            
            // Scan last 1000 lines for efficiency but enough depth
            int scanDepth = 1000;
            int start = Math.max(0, lines.size() - scanDepth);
            
            for (int i = lines.size() - 1; i >= start; i--) {
                String line = lines.get(i);
                boolean allFound = true;
                for (String expected : expectedSubstrings) {
                    if (!line.contains(expected)) {
                        allFound = false;
                        break;
                    }
                }
                if (allFound) return; // Found a line containing all substrings
            }
            
            // If not found, show the last few lines for debugging
            StringBuilder debugInfo = new StringBuilder();
            debugInfo.append("Log validation failed. Expected substrings: ").append(java.util.Arrays.toString(expectedSubstrings)).append("\n");
            debugInfo.append("Last 10 lines of log:\n");
            int debugStart = Math.max(0, lines.size() - 10);
            for (int i = debugStart; i < lines.size(); i++) {
                debugInfo.append(lines.get(i)).append("\n");
            }
            
            assertThat(false).as(debugInfo.toString()).isTrue();
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate logs", e);
        }
    }

    protected boolean checkLogFor(String... expectedSubstrings) {
        try {
            String logPath = "/home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log";
            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(logPath));
            int scanDepth = 1000;
            int start = Math.max(0, lines.size() - scanDepth);
            
            for (int i = lines.size() - 1; i >= start; i--) {
                String line = lines.get(i);
                boolean allFound = true;
                for (String expected : expectedSubstrings) {
                    if (!line.contains(expected)) {
                        allFound = false;
                        break;
                    }
                }
                if (allFound) return true;
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }
}
