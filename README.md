# Teradata to Trino Export Connector: Technical Guide

This document serves as a comprehensive technical guide for the Teradata to Trino Export Connector. It details the architecture, configuration, implementation specifics, and operational procedures.

---

## 1. System Architecture

The export process follows a multi-stage pipeline designed for high-performance data transfer from Teradata's parallel environment to Trino.

### Data Flow Pipeline:
1.  **Teradata SQL**: A query invokes the `ExportToTrino()` Table Operator.
2.  **C Table Operator (UDF)**: Processes rows on each AMP, serializes them using binary formats (`INDICFMT1`), and communicates via TCP sockets.
3.  **Python Arrow Bridge**: Acts as a high-speed intermediary. It receives raw TCP data, converts it into Apache Arrow batches, and sends them to the Trino Flight Server via the `DoPut` stream.
4.  **Trino Flight Server**: Resides inside the Trino Connector. It receives Arrow batches and stores them in a `DataBufferRegistry`.
5.  **Trino PageSource**: Consumes batches from the buffer and converts them into Trino `Page` objects for the Trino Engine.

### AMP-to-Worker Distribution (No Data Duplication)

The connector ensures that **each Teradata AMP sends data to exactly one Trino worker**, preventing data duplication:

```
Teradata AMPs (128)                     Trino Workers (N)
├── AMP 0  ──────────────────────────► Worker 0 (Bridge on port 9999)
├── AMP 1  ──────────────────────────► Worker 1 (Bridge on port 9999)
├── AMP 2  ──────────────────────────► Worker 2 (Bridge on port 9999)
├── AMP 3  ──────────────────────────► Worker 0  (round-robin)
├── AMP 4  ──────────────────────────► Worker 1
...
└── AMP 127 ─────────────────────────► Worker (127 % N)
```

**Key Implementation Details:**
- **C UDF (Line 173):** Uses `amp_id % ip_count` to deterministically select the target worker
- **Trino Split Manager:** Passes all distinct worker IPs to Teradata as a comma-separated list
- **No Duplication Guarantee:** Each AMP processes its own partition of Teradata data and sends it to exactly one worker

**Multi-Worker Deployment Requirements:**
1. Each Trino worker must run its own Arrow Bridge instance on port 9999
2. The `trino-address` config property is only used in single-worker mode
3. In multi-worker mode, actual worker hostnames from `NodeManager` are used

---

## 2. Environment & Credentials

### Connectivity Details
| Component | Host/IP | Port | Credentials |
| :--- | :--- | :--- | :--- |
| **Teradata Database** | `192.168.137.128` | `1025` | `dbc` / `dbc` |
| **Trino Coordinator** | `localhost` | `8080` | N/A |
| **Python Bridge (Listen)**| `0.0.0.0` | `9999` | N/A |
| **Trino Flight Server** | `172.27.251.157` | `50051`| N/A |

### Key Paths
- **Trino Server**: `/home/vijay/tdconnector/trino_server/trino-server-479`
- **JDK Home**: `/home/vijay/tdconnector/trino_server/jdk-25.0.1`
- **Teradata UDF Source**: `/home/vijay/tdconnector/src/teradata/export_to_trino.c`
- **Python Bridge**: `/home/vijay/tdconnector/src/teradata/arrow_bridge.py`
- **Java Connector Source**: `/home/vijay/tdconnector/src/trino/src/main/java/io/trino/plugin/teradata/export/`

---

## 4. Security & Authentication Design

The connector implements a **Hybrid Authentication Model** to balance enterprise security with operational visibility.

### 4.1 Hybrid Authentication Levels
1.  **Metadata (Service-Level)**: Operations such as listing schemas, tables, and describing columns use the **Service Account** (configured via `teradata.user`). This ensures that the Trino optimizer can always discover tables and plan queries regardless of the end-user's specific metadata permissions in Teradata.
2.  **Data Access (Proxy-Level)**: Actual data retrieval queries (the `SELECT` statements that invoke the Table Operator) are executed using a **Proxy Connection**. This ensures that data access is strictly governed by the end-user's permissions in Teradata.

### 4.2 Proxy Mechanism Details
The `TeradataConnectionFactory` is responsible for session initialization:
- When a data query is triggered, it retrieves the Trino session user.
- It executes: `SET QUERY_BAND = 'PROXYUSER=<trino_user>;' FOR SESSION;` immediately after connecting.
- **Strict Enforcement**: If the `SET QUERY_BAND` command fails (e.g., due to missing permissions or invalid user), the connection is **immediately aborted** and an `Access Denied` error is returned to Trino. There is **no fallback** to the service account for data queries.

### 4.3 Teradata Security Requirements
For proxy authentication to work, a Teradata Administrator must grant the following permissions:

```sql
-- Grant connect through rights to the service account
GRANT CONNECT THROUGH <service_user> TO <trino_user> WITHOUT ROLE;

-- Example for user 'vijay':
-- GRANT CONNECT THROUGH sysdba TO vijay WITHOUT ROLE;
```

If the user lacks these rights, they will see an error: `[Error 9203] Connect Through has not been granted to <USER> through <SERVICE_USER>`.

---

### 3.1 C Table Operator (`export_to_trino.c`)
The UDF is the most critical piece for data integrity.

- **Signature**: Uses `void ExportToTrino(void)` with explicit `FNC_TblOpOpen` calls. This ensures a stable stream state and prevents "invalid stream state" errors during complex Trino executions.
- **Binary Codes**: Do not rely on mock headers. Use these confirmed real Teradata internal codes:
    - `TD_VARCHAR`: 2
    - `TD_BYTEINT`: 7
    - `TD_SMALLINT`: 8
    - `TD_INTEGER`: 9
    - `TD_FLOAT`: 10
    - `TD_DECIMAL`: 14
    - `TD_DATE`: 15
    - `TD_TIME`: 16
    - `TD_TIMESTAMP`: 17
    - `TD_BIGINT`: 36
- **Temporal Decoding**: 
    - **TIME**: 6-byte binary. Decoding: `[SecScaled(4)][Hour(1)][Min(1)]`. Seconds are scaled by 1,000,000.
    - **TIMESTAMP**: 10-byte binary. Decoding: `[SecScaled(4)][Year(2)][Month(1)][Day(1)][Hour(1)][Min(1)]`.
    - **DATE Support (0001-01-01)**: The C UDF handles pre-1900 dates by correctly processing negative internal offsets. Years before 1900 (where `d < 0`) are handled via logic: `year = (d/10000) + 1900` with adjustment for negative remainders.
- **Unicode Support**: Character set `UNICODE` (Code 2/6) is stored as UTF-16LE. The UDF contains a manual UTF-16LE to UTF-8 conversion engine. To load test data with multibyte characters (Thai, Chinese), use `bteq -c UTF8`.
- **Timezone Correction**: Teradata sends TIME/TIMESTAMP as local time strings. The Java connector corrects this using the configured `teradata.timezone` (e.g., `-05:00`). It converts from the Teradata server's timezone to the appropriate local representation.

### 3.2 Python Arrow Bridge (`arrow_bridge.py`)
- **Handshake**: Receives `QueryID` and a JSON Column Metadata string from the C UDF.
- **Batching**: Collects rows up to `BATCH_SIZE` (default 1000) before finalizing an Arrow batch.
- **Serialization**: Bit-casts doubles and integers using `struct.pack` to match Teradata's internal representations before placing them into Arrow vectors.

### 3.3 Trino Connector (Java)
- **`TrinoExportSplitManager`**: Orchestrates the process. When `getSplits` is called, it triggers the Teradata SQL execution in a background thread.
- **`DataBufferRegistry`**: Uses a `ConcurrentHashMap` of `BlockingQueue<BatchContainer>`. It implements an **Alias System** where `default-query` (sent by C UDFs) is dynamically routed to the active Trino `QueryID`.
- **`TrinoExportPageSource`**: Implementation of `convertToPage` iterates through Arrow vectors and performs final type-specific writing (e.g., parsing date/decimal strings).

---

### 4.1 Development & Deployment

**Build and Deploy Connector:**
Use this to compile the Java code and update the Trino plugin directory.
```bash
export JAVA_HOME=/home/vijay/tdconnector/trino_server/jdk-25.0.1
export PATH=$JAVA_HOME/bin:$PATH
cd src/trino
mvn clean package dependency:copy-dependencies -DskipTests
export TRINO_HOME=/home/vijay/tdconnector/trino_server/trino-server-479
/bin/bash ../../scripts/deploy_to_trino.sh
```

**Full Cycle (Build + Deploy + JDBC + Restart):**
The most frequently used command during development.
```bash
export JAVA_HOME=/home/vijay/tdconnector/trino_server/jdk-25.0.1 && \
export PATH=$JAVA_HOME/bin:$PATH && \
cd src/trino && mvn clean package dependency:copy-dependencies -DskipTests && \
export TRINO_HOME=/home/vijay/tdconnector/trino_server/trino-server-479 && \
/bin/bash ../../scripts/deploy_to_trino.sh && \
cp /home/vijay/tdconnector/trino_server/terajdbc4.jar $TRINO_HOME/plugin/teradata-export/ && \
$TRINO_HOME/bin/launcher restart --etc-dir=$TRINO_HOME/etc
```

**Restart Trino Server Only:**
```bash
/home/vijay/tdconnector/trino_server/trino-server-479/bin/launcher restart \
  --etc-dir=/home/vijay/tdconnector/trino_server/trino-server-479/etc
```

### 4.2 Teradata Side Management

**Register/Reload UDF:**
```bash
export TD_HOME=/opt/teradata/client/20.00
export PATH=$PATH:$TD_HOME/bin
bteq < scripts/register.bteq
```

**Manual UDF Test (Via BTEQ):**
Useful for isolating UDF issues from Trino.
```bash
bteq <<EOF
.LOGON 192.168.137.128/dbc,dbc
DATABASE TrinoExport;
SELECT * FROM ExportToTrino(
  ON (SELECT TOP 1 * FROM DBC.Tables)
  ON (SELECT '172.27.251.157:9999' as target_ips, 'manual-test' as qid) DIMENSION
) AS t;
.QUIT
EOF
```

### 4.3 Python Bridge Management

**Restart Bridge:**
```bash
pkill -f arrow_bridge.py
nohup /home/vijay/anaconda3/bin/python3 src/teradata/arrow_bridge.py \
  --listen-port 9999 \
  --trino-host 172.27.251.157 \
  --trino-port 50051 > bridge.log 2>&1 &
```

**Watch Bridge Activity:**
```bash
tail -f bridge.log
```

### 4.4 Verification & Testing

#### 4.4.1 Quick Query Test
For rapid validation of specific fixes (like TIME decoding or Decimal precision), use `quick_test.sh`. It automatically filters out `jline` terminal noise and compares results.

```bash
# General query
./tests/quick_test.sh "SELECT current_timestamp"

# Query with expected result comparison
./tests/quick_test.sh "SELECT test_id FROM test_unicode WHERE test_id = 1" "1"
```

#### 4.4.2 Full Integration Suite
The comprehensive suite `run_connector_tests.sh` validates 90+ scenarios across all data types, JOINs, aggregations, and pushdown optimizations.

```bash
# Execute full suite
bash tests/run_connector_tests.sh
```

#### 4.4.3 Log-Based Pushdown Verification
To ensure that filters and limits are truly executed by Teradata (and not just filtered by Trino after retrieval), the suite performs log grep validation.

```bash
# Manual check for specific pushdown in server.log
grep "Executing Teradata SQL" /home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log | tail -n 5

# Validation of Dynamic Filters
grep "Dynamic filter predicate" /home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log | tail -n 5
```

### 4.5 Test Suite Architecture (Integrity Checking)
The test suite was developed using a modular bash framework to ensure reliable connection and data integrity checking.

- **`setup_test_tables.bteq`**: A pre-requisite script that populates Teradata with edge-case data (multibyte Unicode, 0001-01-01 dates, high-precision decimals).
  ```bash
  export TD_HOME=/opt/teradata/client/20.00
  bteq -c UTF8 < tests/setup_test_tables.bteq
  ```
- **Helper Functions**:
  - `run_count_test`: Validates that the number of rows transferred matches Teradata expectation.
  - `run_value_test`: Performs deep value comparison for specific columns, critical for data type decoding validation.
- **Log Markers**: Uses timestamp markers or `tail` offsets to isolate the effects of the *current* query within the shared `server.log`, preventing false positives from previous executions.

### 4.5 Monitoring & Logs

**Watch Trino Server Logs:**
```bash
tail -f /home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log
```

**Search for Teradata Execution SQL in Logs:**
```bash
grep "Executing Teradata SQL" /home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log
```

### Verify System Data (Unicode/DBC)
```bash
-- Use this to verify UTF-16 to UTF-8 conversion
SELECT DatabaseName, CommentString FROM tdexport.dbc.databases LIMIT 5;
```

---


## 5. Troubleshooting Common Issues

1.  **"Invalid Stream State" (Error 7813)**: 
    - **Cause**: Teradata Table Operator was likely registered with parameters in the signature or used an old stream opening pattern.
    - **Fix**: Re-register `ExportToTrino()` using the void signature and explicit `FNC_TblOpOpen(0, 'r', 0)` calls.

2.  **"Failed to convert value ... to type time(6)"**:
    - **Cause**: Binary structure of TIME was misunderstood (likely treated as a double).
    - **Fix**: Use the 6-byte binary decoding logic: `[SecScaled][Hour][Min]`.

3.  **Non-readable/Hex junk in strings**:
    - **Cause**: Character Set UNICODE (UTF-16) was sent directly to Trino (UTF-8).
    - **Fix**: The C UDF must perform UTF-16 to UTF-8 conversion before sending.

4.  **No data in Trino / Query hangs**:
    - **Cause**: Bridge is not running or firewall/IP mismatch. 
    - **Check**: `tail -f bridge_restarted.log` and verify the `TargetIPs` used in the UDF call (sent by Trino `SplitManager`).

---

## 6. Security & Authentication

The connector features an enterprise-grade security model:

### Proxy Authentication (Identity Propagation)
- **Hybrid Model**: Metadata exploration uses the service account, while data execution uses personal proxy accounts.
- **Set Query Band**: Personalizes the JDBC session using `SET QUERY_BAND = 'PROXYUSER=<user>;'`.
- **Strict Mode**: Quits and fails the query if proxy authorization fails.

### Secure Credentials
- **Script Support**: Retrieve passwords and tokens from external scripts (e.g., vault integrations) to avoid clear-text storage.
- **Tokenized Bridge**: The socket channel between Teradata and the bridge is secured with a shared secret token.

---

## 7. Catalog Configuration (`tdexport.properties`)

### Basic Configuration
```properties
connector.name=teradata-export
teradata.url=jdbc:teradata://192.168.137.128/DATABASE=TrinoExport
teradata.user=sysdba
teradata.password=sysdba
flight.port=50051

# Timezone Adjustment (Teradata server timezone)
teradata.timezone=-05:00
```

### Enterprise Security (Script-based Credentials)
To avoid storing clear-text passwords or security tokens in the configuration file, you can provide an external script path. The connector will execute the script and use its first line of output as the credential.

```properties
# Instead of teradata.password
teradata.password-script=/path/to/get_password.sh

# Instead of teradata.export.token
teradata.export.token-script=/path/to/get_token.sh
```

**Security Requirements:**
- The script must be executable by the user running the Trino process.
- The script should only output the secret value (e.g., via `echo`).
- If both a direct value and a script are provided, the script takes precedence.

### Dynamic Filtering
```properties
teradata.export.enable-dynamic-filtering=true
teradata.export.dynamic-filter-timeout=10s
```

---
*Created on 2025-12-23*
