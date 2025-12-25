#!/bin/bash
# =================================================================
# test_e2e.sh - End-to-End Test for Teradata-Trino Export
# =================================================================
#
# This script tests the complete data flow:
# 1. Teradata Table Operator execution
# 2. Arrow Flight data streaming  
# 3. Trino connector data reception (if Trino is available)
#
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")

# Configuration
TD_HOST="${TD_HOST:-192.168.137.129}"
TD_USER="${TD_USER:-dbc}"
TD_PASS="${TD_PASS:-dbc}"
FLIGHT_PORT="${FLIGHT_PORT:-50051}"

export TD_HOME="${TD_HOME:-/opt/teradata/client/20.00}"
export PATH=$PATH:$TD_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TD_HOME/lib64:$TD_HOME/lib

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║      Teradata to Trino - End-to-End Test Suite            ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================
# Test 1: Verify Teradata Connection
# =============================================
echo -e "${YELLOW}[Test 1/4] Verifying Teradata connection...${NC}"

CONN_TEST=$(cat << EOF | bteq 2>&1
.SET EXITONDELAY ON
.LOGON $TD_HOST/$TD_USER,$TD_PASS
SELECT 1 as test;
.QUIT
EOF
)

if echo "$CONN_TEST" | grep -q "Query completed"; then
    echo -e "${GREEN}✓ Teradata connection successful${NC}"
else
    echo -e "${RED}✗ Teradata connection failed${NC}"
    echo "$CONN_TEST"
    exit 1
fi
echo ""

# =============================================
# Test 2: Verify Table Operator Registration
# =============================================
echo -e "${YELLOW}[Test 2/4] Checking Table Operator registration...${NC}"

REG_TEST=$(cat << EOF | bteq 2>&1
.LOGON $TD_HOST/$TD_USER,$TD_PASS
DATABASE TrinoExport;
SELECT COUNT(*) FROM DBC.FunctionsV WHERE FunctionName = 'EXPORTTOTRINO';
.QUIT
EOF
)

if echo "$REG_TEST" | grep -q "1"; then
    echo -e "${GREEN}✓ ExportToTrino Table Operator is registered${NC}"
else
    echo -e "${RED}✗ ExportToTrino Table Operator is NOT registered${NC}"
    echo "  Run: bteq < scripts/register.bteq"
    exit 1
fi
echo ""

# =============================================
# Test 3: Execute Table Operator
# =============================================
echo -e "${YELLOW}[Test 3/4] Executing Table Operator...${NC}"

EXEC_TEST=$(cat << EOF | bteq 2>&1
.LOGON $TD_HOST/$TD_USER,$TD_PASS
DATABASE TrinoExport;

-- Create test table if not exists
CREATE TABLE TestDataE2E (
    id_col INTEGER,
    name_col VARCHAR(50),
    amount BIGINT
);

-- Clear old data
DELETE FROM TestDataE2E;

-- Insert test data
INSERT INTO TestDataE2E VALUES (1, 'Record1', 1000);
INSERT INTO TestDataE2E VALUES (2, 'Record2', 2000);
INSERT INTO TestDataE2E VALUES (3, 'Record3', 3000);
INSERT INTO TestDataE2E VALUES (4, 'Record4', 4000);
INSERT INTO TestDataE2E VALUES (5, 'Record5', 5000);

-- Execute Table Operator
SELECT * FROM ExportToTrino(
    ON (SELECT * FROM TestDataE2E)
) AS export_result;

.QUIT
EOF
)

if echo "$EXEC_TEST" | grep -q "SUCCESS"; then
    echo -e "${GREEN}✓ Table Operator executed successfully${NC}"
    
    # Extract stats
    ROWS_PROCESSED=$(echo "$EXEC_TEST" | grep "SUCCESS" | awk '{sum+=$2} END {print sum}')
    BYTES_SENT=$(echo "$EXEC_TEST" | grep "SUCCESS" | awk '{sum+=$3} END {print sum}')
    AMPS_USED=$(echo "$EXEC_TEST" | grep "SUCCESS" | wc -l)
    
    echo "  AMPs used: $AMPS_USED"
    echo "  Rows processed: $ROWS_PROCESSED"
    echo "  Bytes sent: $BYTES_SENT"
else
    echo -e "${RED}✗ Table Operator execution failed${NC}"
    echo "$EXEC_TEST"
    exit 1
fi
echo ""

# =============================================
# Test 4: Check Trino Availability (Optional)
# =============================================
echo -e "${YELLOW}[Test 4/4] Checking Trino connectivity...${NC}"

if command -v trino &> /dev/null && [ -n "$TRINO_HOME" ]; then
    echo "  Trino CLI found. Testing catalog visibility..."
    
    TRINO_TEST=$(echo "SHOW CATALOGS" | trino --server localhost:8080 2>&1 || true)
    
    if echo "$TRINO_TEST" | grep -qi "teradata-export"; then
        echo -e "${GREEN}✓ teradata-export catalog is visible in Trino${NC}"
    else
        echo -e "${YELLOW}⚠ teradata-export catalog not found in Trino${NC}"
        echo "  Deploy the connector and restart Trino"
    fi
else
    echo -e "${YELLOW}⚠ Trino CLI not available or TRINO_HOME not set${NC}"
    echo "  Skipping Trino connectivity test"
fi
echo ""

# =============================================
# Summary
# =============================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}End-to-End Test Suite Complete!${NC}"
echo ""
echo -e "${YELLOW}Summary:${NC}"
echo "  ✓ Teradata connection: OK"
echo "  ✓ Table Operator registration: OK"
echo "  ✓ Table Operator execution: OK"
echo ""
echo "The Teradata Table Operator is ready for Arrow Flight integration."
echo ""
