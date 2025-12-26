package io.trino.tests.tdexport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Section 15 & 16: Log-based Pushdown Validation")
public class LogValidationTest extends BaseTdExportTest {

    private static final String LOG_FILE = "/home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log";

    @Test
    @DisplayName("15.2 Integer filter pushed to Teradata")
    public void test15_2() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_int = 100", "2");
        Thread.sleep(1000);
        assertLogContains("Executing Teradata SQL", "filter_int = 100");
    }

    @Test
    @DisplayName("15.4 VARCHAR filter pushed to Teradata")
    public void test15_4() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_pushdown WHERE filter_varchar = 'alpha'", "2");
        Thread.sleep(1000);
        assertLogContains("Executing Teradata SQL", "filter_varchar = 'alpha'");
    }

    @Test
    @DisplayName("15.6 LIMIT pushed as SAMPLE to Teradata")
    public void test15_6() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM test_pushdown LIMIT 3)", "3");
        Thread.sleep(1000);
        assertLogContains("Executing Teradata SQL", "SAMPLE 3");
    }

    @Test
    @DisplayName("16.2 Dynamic filter applied")
    public void test16_2() throws IOException, InterruptedException {
        assertQuery("SELECT COUNT(*) FROM test_join_fact f JOIN test_join_dim d ON f.dim_id = d.dim_id WHERE d.dim_category = 'Type1'", "5");
        Thread.sleep(1000);
        boolean foundInPredicateLog = checkLogFor("Dynamic filter predicate", "dim_id IN");
        boolean foundInSqlLog = checkLogFor("Executing Teradata SQL", "dim_id IN");
        assertThat(foundInPredicateLog || foundInSqlLog).as("Dynamic filter not found in logs").isTrue();
    }

    private void assertLogContains(String marker, String expected) throws IOException {
        assertThat(checkLogFor(marker, expected)).as("Log missing: " + expected).isTrue();
    }

    private boolean checkLogFor(String marker, String expected) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(LOG_FILE));
        int start = Math.max(0, lines.size() - 1000);
        for (int i = lines.size() - 1; i >= start; i--) {
            String line = lines.get(i);
            if (line.contains(marker) && line.contains(expected)) return true;
        }
        return false;
    }
}
