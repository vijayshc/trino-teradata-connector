# Teradata to Trino High-Performance Export Connector

A high-performance, massively parallel data export solution from Teradata to Trino using Apache Arrow Flight.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    TERADATA DATABASE                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                         ExportToTrino Table Operator (C)                        │   │
│   │                                                                                  │   │
│   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │   │
│   │  │    AMP 0     │  │    AMP 1     │  │    AMP 2     │  │    AMP N     │         │   │
│   │  │              │  │              │  │              │  │              │         │   │
│   │  │ Read Rows    │  │ Read Rows    │  │ Read Rows    │  │ Read Rows    │         │   │
│   │  │ ↓            │  │ ↓            │  │ ↓            │  │ ↓            │         │   │
│   │  │ Arrow Batch  │  │ Arrow Batch  │  │ Arrow Batch  │  │ Arrow Batch  │         │   │
│   │  │ ↓            │  │ ↓            │  │ ↓            │  │ ↓            │         │   │
│   │  │ Flight DoPut │  │ Flight DoPut │  │ Flight DoPut │  │ Flight DoPut │         │   │
│   │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │   │
│   └─────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┘   │
└─────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────────┘
              │                 │                 │                 │
              │ Arrow Flight    │ Arrow Flight    │ Arrow Flight    │ Arrow Flight
              │ gRPC Stream     │ gRPC Stream     │ gRPC Stream     │ gRPC Stream
              ↓                 ↓                 ↓                 ↓
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                     TRINO CLUSTER                                        │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│   │   Worker 0   │  │   Worker 1   │  │   Worker 2   │  │   Worker N   │                │
│   │              │  │              │  │              │  │              │                │
│   │ Flight Server│  │ Flight Server│  │ Flight Server│  │ Flight Server│                │
│   │ ↓            │  │ ↓            │  │ ↓            │  │ ↓            │                │
│   │ Buffer Data  │  │ Buffer Data  │  │ Buffer Data  │  │ Buffer Data  │                │
│   │ ↓            │  │ ↓            │  │ ↓            │  │ ↓            │                │
│   │ PageSource   │  │ PageSource   │  │ PageSource   │  │ PageSource   │                │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                │
│          └──────────────────┼────────────────┴──────────────────┘                        │
│                             ↓                                                            │
│                    ┌─────────────────┐                                                   │
│                    │  Trino Query    │                                                   │
│                    │  Coordinator    │                                                   │
│                    └─────────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

1. **Massively Parallel Processing**: The Table Operator runs on all Teradata AMPs simultaneously, allowing parallel data export from each AMP.

2. **Apache Arrow**: Data is converted to Apache Arrow's columnar format for efficient in-memory processing and zero-copy transfers.

3. **Arrow Flight**: High-performance gRPC-based data streaming protocol optimized for transferring large datasets.

4. **Push-Based Architecture**: The Teradata Table Operator pushes data directly to Trino workers, bypassing traditional JDBC bottlenecks.

## Project Structure

```
tdconnector/
├── src/
│   ├── teradata/
│   │   ├── export_to_trino.c       # C Table Operator (contract + execution)
│   │   └── include/
│   │       └── sqltypes_td.h       # Teradata type definitions
│   └── trino/
│       └── src/main/java/io/trino/plugin/teradata/export/
│           ├── TrinoExportPlugin.java
│           ├── TrinoExportConnector.java
│           ├── TrinoExportConnectorFactory.java
│           ├── TrinoExportModule.java
│           ├── TrinoExportConfig.java
│           ├── TrinoExportMetadata.java
│           ├── TrinoExportSplitManager.java
│           ├── TrinoExportPageSource.java
│           ├── TrinoExportFlightServer.java
│           └── DataBufferRegistry.java
├── scripts/
│   ├── register.bteq               # BTEQ script to register Table Operator
│   ├── test_table_operator.sh      # Test the Table Operator
│   └── deploy_to_trino.sh          # Deploy connector to Trino
├── tests/
│   ├── run_java_suite.sh          # Modular JDBC-based test suite (Recommended)
│   ├── run_connector_tests.sh     # Legcay bash-based test suite
│   ├── quick_test.sh              # Quick result verification
│   └── trino-tests/               # Java/JUnit testing source
├── config/
│   └── teradata-export.properties  # Trino catalog configuration
└── docs/
    └── ISSUES_ANALYSIS.md          # Technical analysis and troubleshooting
```

## Prerequisites

### Teradata Server
- Teradata Database 15.0+
- C compiler (gcc) on the database server
- CREATE FUNCTION permissions

### Trino Server
- Trino 420+ (Java 17)
- Apache Arrow libraries

### Build Environment
- Java 17+ (JDK)
- Maven 3.8+
- Teradata client tools (for BTEQ)

## Quick Reference: Frequently Used Commands

For detailed operational guidance, see the [Technical Guide](docs/TECHNICAL_GUIDE_TDEXPORT.md).

### Build & Deploy (Full Cycle)
```bash
export JAVA_HOME=/home/vijay/tdconnector/trino_server/jdk-25.0.1
export TRINO_HOME=/home/vijay/tdconnector/trino_server/trino-server-479
export PATH=$JAVA_HOME/bin:$PATH

cd src/trino && mvn clean package dependency:copy-dependencies -DskipTests && \
/bin/bash ../../scripts/deploy_to_trino.sh && \
cp /home/vijay/tdconnector/trino_server/terajdbc4.jar $TRINO_HOME/plugin/teradata-export/ && \
$TRINO_HOME/bin/launcher restart --etc-dir=$TRINO_HOME/etc
```

### Restart Python Bridge
```bash
pkill -f arrow_bridge.py
nohup /home/vijay/anaconda3/bin/python3 src/teradata/arrow_bridge.py \
  --listen-port 9999 --trino-host 172.27.251.157 --trino-port 50051 > bridge.log 2>&1 &
```

### Run Tests
```bash
# Modular Java Suite (Recommended: Faster, Modular)
/bin/bash tests/run_java_suite.sh

# Full Legacy Bash Suite
bash tests/run_connector_tests.sh

# Single Query Check
./tests/quick_test.sh "SELECT COUNT(*) FROM tdexport.trinoexport.TestData"
```

### Monitor Logs
```bash
# Trino Logs
tail -f /home/vijay/tdconnector/trino_server/trino-server-479/data/var/log/server.log

# Bridge Logs
tail -f bridge.log
```

## Usage

### From Trino CLI

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Use the Teradata Export catalog
USE tdexport.TrinoExport;

-- Show tables
SHOW TABLES;

-- Query data (triggers parallel export from Teradata)
SELECT * FROM LargeTable LIMIT 100;
```

### Direct Table Operator Usage (BTEQ)

```sql
-- Verify registration
SELECT FunctionName, FunctionType FROM DBC.FunctionsV 
WHERE FunctionName LIKE 'EXPORT%';

-- Execute Table Operator
SELECT * FROM ExportToTrino(
    ON (SELECT * FROM MyDatabase.MyTable)
) AS export_result;
```

## Performance Tuning

### Teradata Side

1. **Batch Size**: Adjust `BATCH_SIZE` in `export_to_trino.c` (default: 10,000 rows per Arrow batch)

2. **Memory**: Each AMP allocates memory for Arrow batches; monitor with Teradata system views

### Trino Side

1. **Flight Port**: Ensure port 50051 is open between Teradata and Trino workers

2. **Buffer Size**: Adjust `DataBufferRegistry` queue size for memory vs. throughput tradeoff

## Troubleshooting

### Table Operator Registration Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Failure 3707: Syntax error` | Incorrect DDL syntax | Use `LANGUAGE C` and `PARAMETER STYLE SQLTable` |
| `Failure 3524: No CREATE FUNCTION access` | Missing permissions | `GRANT CREATE FUNCTION ON <database> TO <user>` |
| `Function already exists` | Duplicate registration | Use `REPLACE FUNCTION` instead of `CREATE FUNCTION` |

### Runtime Errors

| Error | Cause | Solution |
|-------|-------|----------|
| No data received in Trino | Network/firewall | Check port 50051 connectivity |
| Connection refused | Flight server not started | Verify Trino worker startup logs |
| Memory exhaustion | Large batch sizes | Reduce `BATCH_SIZE` |

## License

MIT License - See LICENSE file for details.

## Contributors

- Teradata Table Operator development using Teradata External Routine Programming guide
- Trino Connector based on Trino SPI documentation
