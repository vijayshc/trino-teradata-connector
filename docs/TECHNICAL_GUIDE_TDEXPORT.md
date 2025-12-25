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

### Multi-Worker Architecture Details

**Split Locality Enforcement:**
- Each split is assigned to a specific worker via `getAddresses()`
- `isRemotelyAccessible()` returns `false` to enforce local execution only
- This ensures data sent to Worker N is processed by Worker N's PageSource

**Per-Worker Buffer Registration:**
- Each worker runs its own `DataBufferRegistry` (static class, but per-JVM)
- `PageSource` constructor registers the query buffer on that worker
- Data arriving before `PageSource` is auto-buffered (race condition handling)

**End-of-Stream Detection:**
- EOS is detected when all socket connections close (not JDBC-based)
- Each worker independently detects EOS for its portion of data
- No coordinator-to-worker signaling required

**Hostname-to-IP Resolution:**
- Teradata C UDF uses `inet_pton()` which requires IP addresses
- Split Manager resolves worker hostnames to IP addresses automatically
- For NAT/multi-homed networks, use `worker-advertised-addresses` config

**Multi-Worker Deployment Requirements:**
1. Each Trino worker runs its own Bridge Server instance on the configured port
2. The `trino-address` config property is only used in single-worker mode
3. In multi-worker mode, worker IPs are auto-discovered from `NodeManager`
4. For NAT environments, configure `worker-advertised-addresses` explicitly

---

## 2. Environment & Credentials

### Connectivity Details
| Component | Host/IP | Port | Credentials |
| :--- | :--- | :--- | :--- |
| **Teradata Database** | `192.168.137.129` | `1025` | `dbc` / `dbc` |
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
.LOGON 192.168.137.129/dbc,dbc
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

#### 4.4.4 Modular Java Test Suite (Recommended)
For deep, modular, and faster validation, a Java-based test suite using JUnit 5 and the Trino JDBC driver is available. This suite allows running specific test categories or individual cases.

**Advantages:**
- **Speed**: Persistent JDBC connections and optimized query execution.
- **Modularity**: Tests are grouped by type (Numeric, Char, DateTime, Complex, etc.).
- **Granular Execution**: Can run a single test class or even a single test method.
- **Rich Reporting**: Detailed diffs on failure via AssertJ.

**Prerequisites:**
- Trino JDBC driver in `/home/vijay/tdconnector/trino_server/trino-jdbc-479.jar`
- JDK 21+ and Maven.

**Running the Java Suite:**
```bash
# Run all 90+ validations
/bin/bash tests/run_java_suite.sh

# Run specific functional group (e.g., Numeric types only)
/bin/bash tests/run_java_suite.sh NumericDataTypeTest

# Run a specific test method within a class
/bin/bash tests/run_java_suite.sh LogValidationTest#test15_6
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

The connector is highly configurable to support various enterprise environments. Below is a comprehensive reference of all configuration properties.

### 7.1 Core Connection Settings

```properties
connector.name=teradata-export
teradata.url=jdbc:teradata://192.168.137.129/DATABASE=TrinoExport
teradata.user=sysdba
teradata.password=sysdba

# Teradata server timezone offset (for TIME/TIMESTAMP conversion)
# Format: +/-HH:MM (e.g., -05:00 for EST, +08:00 for SGT)
teradata.timezone=-05:00
```

### 7.2 Network/Port Settings

```properties
# Arrow Flight server port (for gRPC-based data transfer)
teradata.export.flight-port=50051

# Arrow Flight server bind address (network interface)
# Use 0.0.0.0 to listen on all interfaces
teradata.export.flight-bind-address=0.0.0.0

# Java Bridge server port (for direct TCP connections from Teradata AMPs)
teradata.export.bridge-port=9999

# Trino coordinator/worker address (used in single-worker mode)
# In multi-worker mode, actual worker addresses are automatically discovered
teradata.export.trino-address=172.27.251.157
```

### 7.3 Buffer/Performance Settings

```properties
# TCP socket receive buffer size in bytes (default: 4MB)
# Increase for high-latency networks or large batch sizes
teradata.export.socket-receive-buffer-size=4194304

# Input stream buffer size in bytes (default: 1MB)
# Buffer for reading data from Teradata connections
teradata.export.input-buffer-size=1048576

# Maximum number of Arrow batches buffered per query (default: 100)
# Increase for larger data volumes, decrease to limit memory usage
teradata.export.buffer-queue-capacity=100

# Timeout in milliseconds for polling data from buffer (default: 500)
# Lower values improve responsiveness, higher values reduce CPU usage
teradata.export.page-poll-timeout-ms=500
```

### 7.4 Dynamic Filtering Settings

```properties
# Timeout for waiting for dynamic filters to be collected
teradata.export.dynamic-filter-timeout=10s

# Enable or disable dynamic filter pushdown to Teradata
teradata.export.enable-dynamic-filtering=true
```

### 7.5 Security Settings

```properties
# Security Token for Socket Channel Authentication
# Provides an extra layer of security between the Teradata UDF and the Trino bridge
teradata.export.token=enterprise-secret-123

# Enforce proxy authentication strictly (default: true)
# When true, queries fail if QUERY_BAND cannot be set for the user
# When false, queries continue with a warning if proxy auth fails
teradata.export.enforce-proxy-authentication=true
```

**Script-Based Credentials:**
To avoid storing clear-text passwords or tokens, use external scripts:

```properties
# Script to fetch password (takes precedence over teradata.password)
teradata.password-script=/path/to/get_password.sh

# Script to fetch security token (takes precedence over teradata.export.token)
teradata.export.token-script=/path/to/get_token.sh
```

### 7.6 UDF Location Settings

```properties
# Database where ExportToTrino UDF is installed
# Default: TrinoExport (can be changed to td_syslib or any custom database)
teradata.export.udf-database=TrinoExport

# Name of the UDF function (default: ExportToTrino)
# Change if the UDF is registered with a different name
teradata.export.udf-name=ExportToTrino
```

### 7.7 Schema Settings

```properties
# Default schemas to always include in schema list (comma-separated)
# These schemas appear even if not discoverable from Teradata metadata
teradata.export.default-schemas=TrinoExport,default
```

### 7.8 Logging Settings

```properties
# Enable detailed debug logging for type conversion and data processing
# Warning: This generates verbose logs and should only be enabled for troubleshooting
teradata.export.enable-debug-logging=false
```

---

## 8. Configuration Properties Reference

| Property | Default | Description |
|:---------|:--------|:------------|
| `teradata.url` | *required* | JDBC URL for Teradata connection |
| `teradata.user` | *required* | Teradata service account username |
| `teradata.password` | *required* | Teradata service account password |
| `teradata.password-script` | *none* | Script to fetch password dynamically |
| `teradata.timezone` | `-05:00` | Teradata server timezone offset |
| `teradata.export.flight-port` | `50051` | Arrow Flight server port |
| `teradata.export.flight-bind-address` | `0.0.0.0` | Arrow Flight bind address |
| `teradata.export.bridge-port` | `9999` | Java Bridge server port |
| `teradata.export.trino-address` | *required* | Trino address for single-worker mode |
| `teradata.export.worker-advertised-addresses` | *none* | Worker IPs for NAT/multi-homed networks |
| `teradata.export.socket-receive-buffer-size` | `4194304` | Socket receive buffer (bytes) |
| `teradata.export.input-buffer-size` | `1048576` | Input stream buffer (bytes) |
| `teradata.export.buffer-queue-capacity` | `100` | Max batches buffered per query |
| `teradata.export.page-poll-timeout-ms` | `500` | Buffer poll timeout (ms) |
| `teradata.export.dynamic-filter-timeout` | `10s` | Dynamic filter wait timeout |
| `teradata.export.enable-dynamic-filtering` | `true` | Enable dynamic filter pushdown |
| `teradata.export.token` | *none* | Security token for bridge auth |
| `teradata.export.token-script` | *none* | Script to fetch token dynamically |
| `teradata.export.enforce-proxy-authentication` | `true` | Strictly enforce proxy auth |
| `teradata.export.udf-database` | `TrinoExport` | Database where UDF is installed |
| `teradata.export.udf-name` | `ExportToTrino` | Name of the UDF function |
| `teradata.export.default-schemas` | `TrinoExport,default` | Default schemas to include |
| `teradata.export.enable-debug-logging` | `false` | Enable verbose debug logging |

---

## 9. Performance & Architectural Optimizations

The Teradata Export Connector is engineered for massive parallel throughput, capable of saturating 25Gbps+ network links. Its performance is achieved through several advanced architectural design patterns.

### 9.1 Internalized Zero-Copy Bridge
Historically, connectors utilized external bridge processes (e.g., Python). This connector internalizes the **Teradata Bridge Server** directly into the Trino Worker JVM.
- **Benefit**: Eliminates cross-process IPC and socket hops.
- **Zero-Copy Handover**: Once data is received by the Bridge, it is placed in a `DataBufferRegistry`. The `PageSource` receives a memory pointer (reference) to this data. No bytes are copied between the "receiver" and the "reader".

### 9.2 Off-Heap Memory Management (Apache Arrow)
To handle millions of rows without triggering excessive Java Garbage Collection (GC) pauses, the connector utilizes **Direct Memory**:
- **Bypassing the Heap**: Data is stored in Apache Arrow `FieldVectors` allocated in off-heap memory.
- **GC Overhead Mitigation**: Since the bulk of the data (Gigabytes per second) lives outside the Java Heap, Trino can maintain high performance without "Stop-the-World" pauses.

### 9.3 Vectorized Data Path
Instead of row-by-row processing, the connector operates on **Batches**:
- **Packed Binary Protocol**: The Teradata C UDF sends packed binary structures directly over TCP.
- **SIMD-Ready Conversion**: Conversion from Arrow vectors to Trino `Blocks` uses optimized primitive loops that the JVM Just-In-Time (JIT) compiler can translate into SIMD (Single Instruction, Multiple Data) instructions.

### 9.4 Massively Parallel Data Routing
The connector leverages the full parallelism of the Teradata cluster:
- **AMP-Level Parallelism**: Every Teradata AMP (Access Module Processor) establishes its own independent socket connection to a Trino worker.
- **Deterministic Load Balancing**: Using `amp_id % worker_count`, data is distributed evenly across the Trino cluster without requiring a central load balancer.
- **Locality Awareness**: Trino Splits are configured with `isRemotelyAccessible = false`, forcing the engine to run the processing logic on the exact worker node where the data is arriving.

### 9.5 Protocol Specialization (Dual-Path)
The connector maintains two specialized intake servers to maximize compatibility and performance:
1.  **Bridge Server (Port 9999)**: Uses a custom, ultra-lightweight TCP protocol optimized for the Teradata C environment.
2.  **Arrow Flight Server (Port 50051)**: Provides a standards-based gRPC/Flight interface for modern data science tools and external clients.

### 9.6 Robust Stream Synchronization (Hybrid EOS)

A critical challenge in parallel data export is accurately determining when a query has finished. The connector implements a **Hybrid EOS (End-of-Stream)** protocol in the `DataBufferRegistry`:

- **Socket State Tracking**: The bridge maintains an atomic counter of active TCP connections from Teradata AMPs. EOS is only considered once this counter reaches zero.
- **JDBC Completion Signal**: Upon finishing the SQL execution on Teradata, the Trino coordinator broadcasts a control signal ("JDBC Finished") to all workers. This prevents the connector from closing too early if some AMPs are delayed in starting.
- **Idle Stabilization Window**: To account for OS-level buffering and network jitter, a **500ms stabilization window** is enforced. The connector must be both connection-free AND finished with JDBC for at least 500ms before it signals the final EOS to the Trino engine.
- **Scheduled Retries**: If the stabilization check fails due to recent activity, it automatically schedules a background retry, ensuring that no data is ever lost even under heavy network congestion.

---

*Updated on 2025-12-24*
