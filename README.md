# Teradata to Trino High-Performance Export Connector

A massively parallel data export connector that enables direct, high-throughput data transfer from Teradata to Trino using Apache Arrow Flight.

## Overview

This connector bypasses traditional JDBC limitations by using Teradata's native parallel processing (AMPs) to push data directly to Trino workers via Apache Arrow Flight protocol.

```
┌─────────────────────────────────────────────────────────────────┐
│                     TERADATA DATABASE                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  AMP 0   │  │  AMP 1   │  │  AMP 2   │  │  AMP N   │        │
│  │    ↓     │  │    ↓     │  │    ↓     │  │    ↓     │        │
│  │ TCP/IP   │  │ TCP/IP   │  │ TCP/IP   │  │ TCP/IP   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
└───────┼─────────────┼─────────────┼─────────────┼───────────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       ARROW BRIDGE                               │
│              (Protocol Translation Layer)                        │
│          Teradata Binary → Apache Arrow Batches                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Arrow Flight
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TRINO CLUSTER                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Arrow Flight Server (Port 50051)             │   │
│  │                          ↓                                │   │
│  │              DataBufferRegistry (QueryID)                 │   │
│  │                          ↓                                │   │
│  │              Trino PageSource → Query Engine              │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Massively Parallel**: Each Teradata AMP exports data independently
- **Zero-Copy Transfer**: Uses Apache Arrow columnar format
- **No Data Duplication**: Deterministic AMP-to-Worker routing
- **Memory Safe**: All resources properly managed across all layers
- **Production Ready**: Handles concurrent queries with isolation

## Prerequisites

### Teradata Server
- Teradata Database 15.0+
- CREATE FUNCTION permissions
- Network access to Trino workers

### Trino Server
- Trino 420+ (Java 17/21)
- Maven 3.8+ (for building)
- Python 3.8+ with PyArrow (for bridge)

### Build Requirements
- JDK 17 or higher
- Maven 3.8+
- Teradata BTEQ client (optional, for UDF registration)

## Project Structure

```
opensource/
├── src/
│   ├── teradata/
│   │   ├── export_to_trino.c     # C Table Operator (runs on Teradata)
│   │   ├── arrow_bridge.py       # Python bridge (runs on Trino workers)
│   │   ├── requirements.txt      # Python dependencies
│   │   └── include/
│   │       └── sqltypes_td.h     # Teradata type definitions
│   └── trino/
│       ├── pom.xml               # Maven build configuration
│       └── src/main/java/...     # Trino connector source
├── dist/
│   └── trino-teradata-export-1.0-SNAPSHOT.jar    # Pre-built connector JAR
├── scripts/
│   ├── register.bteq             # BTEQ script for UDF registration
│   └── deploy_to_trino.sh        # Deployment automation
├── config/
│   └── teradata-export.properties    # Catalog configuration template
├── LICENSE
└── README.md
```

---

## Installation

### Step 1: Build the Trino Connector

```bash
cd opensource/src/trino
mvn clean package dependency:copy-dependencies -DskipTests
```

This produces:
- `target/trino-teradata-export-1.0-SNAPSHOT.jar` (connector)
- `target/dependency/*.jar` (required dependencies)

### Step 2: Deploy to Trino

```bash
export TRINO_HOME=/path/to/trino-server

# Create plugin directory
mkdir -p $TRINO_HOME/plugin/teradata-export

# Copy connector JAR and dependencies
cp src/trino/target/trino-teradata-export-1.0-SNAPSHOT.jar $TRINO_HOME/plugin/teradata-export/
cp src/trino/target/dependency/*.jar $TRINO_HOME/plugin/teradata-export/

# Copy Teradata JDBC driver (required, download from Teradata)
cp /path/to/terajdbc4.jar $TRINO_HOME/plugin/teradata-export/
```

### Step 3: Configure Catalog

Create `$TRINO_HOME/etc/catalog/tdexport.properties`:

```properties
connector.name=teradata-export
teradata.url=jdbc:teradata://YOUR_TERADATA_HOST/DATABASE=YourDatabase
teradata.user=your_user
teradata.password=your_password
teradata.export.flight-port=50051
teradata.export.bridge-port=9999
teradata.export.trino-address=YOUR_TRINO_WORKER_IP
```

### Step 4: Start the Arrow Bridge

On each Trino worker node:

```bash
cd opensource/src/teradata
python3 arrow_bridge.py \
  --listen-port 9999 \
  --trino-host 127.0.0.1 \
  --trino-port 50051
```

For production, run as a systemd service:

```bash
nohup python3 arrow_bridge.py --listen-port 9999 --trino-host 127.0.0.1 --trino-port 50051 > bridge.log 2>&1 &
```

### Step 5: Register Teradata UDF

Edit `scripts/register.bteq` with your Teradata credentials, then:

```bash
export TD_HOME=/opt/teradata/client/20.00
export PATH=$PATH:$TD_HOME/bin
bteq < scripts/register.bteq
```

### Step 6: Restart Trino

```bash
$TRINO_HOME/bin/launcher restart
```

---

## Usage

### Basic Query

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Use the Teradata Export catalog
USE tdexport.your_database;

-- Query data (triggers parallel export from Teradata)
SELECT * FROM your_table LIMIT 100;

-- Aggregate queries
SELECT COUNT(*), SUM(amount) FROM sales_data;
```

### Verify Connector Status

```sql
-- Check if catalog is loaded
SHOW CATALOGS;

-- List schemas (Teradata databases)
SHOW SCHEMAS FROM tdexport;

-- List tables in a schema
SHOW TABLES FROM tdexport.your_database;
```

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.url` | (required) | JDBC connection URL |
| `teradata.user` | (required) | Teradata username |
| `teradata.password` | (required) | Teradata password |
| `teradata.export.flight-port` | `50051` | Arrow Flight server port |
| `teradata.export.bridge-port` | `9999` | Bridge listener port |
| `teradata.export.trino-address` | `127.0.0.1` | Trino worker IP (single-worker mode) |

---

## Multi-Worker Deployment

For clusters with multiple Trino workers:

1. Start a bridge instance on **each worker node**
2. Each bridge listens on port 9999
3. The connector automatically distributes AMPs across workers
4. Formula: `AMP_ID % WORKER_COUNT` determines target worker

```
128 AMPs → 4 Workers:
├── AMPs 0, 4, 8, ... → Worker 0
├── AMPs 1, 5, 9, ... → Worker 1
├── AMPs 2, 6, 10, ... → Worker 2
└── AMPs 3, 7, 11, ... → Worker 3
```

---

## Troubleshooting

### UDF Registration Errors

| Error | Solution |
|-------|----------|
| `Failure 3524: No CREATE FUNCTION access` | Grant permission: `GRANT CREATE FUNCTION ON database TO user` |
| `Function already exists` | Use `REPLACE FUNCTION` instead of `CREATE FUNCTION` |

### Runtime Errors

| Error | Solution |
|-------|----------|
| No data returned | Check bridge is running: `ps aux | grep arrow_bridge` |
| Connection refused | Verify port 9999 is open: `netstat -tlnp | grep 9999` |
| Query timeout | Increase Trino query timeout or reduce data volume |

### Checking Logs

```bash
# Trino server logs
tail -f $TRINO_HOME/data/var/log/server.log

# Bridge logs
tail -f bridge.log
```

---

## Performance Tuning

### Teradata Side
- Adjust `BATCH_SIZE` in `export_to_trino.c` (default: 1000 rows per batch)
- Larger batches = higher throughput, more memory per AMP

### Bridge Side
- Ensure adequate network bandwidth between Teradata and Trino
- Use multiple bridge instances for high-volume workloads

### Trino Side
- Configure adequate heap memory for Arrow buffers
- Monitor `DataBufferRegistry` queue sizes in logs

---

## License

MIT License - See [LICENSE](LICENSE) file for details.

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with tests

---

## Acknowledgments

- Teradata External Routine Programming Guide
- Trino SPI Documentation
- Apache Arrow Flight Protocol
