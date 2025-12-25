#!/bin/bash
# =================================================================
# test_table_operator.sh - Test the Teradata Table Operator
# =================================================================
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")

# Teradata connection info
TD_HOST="${TD_HOST:-192.168.137.129}"
TD_USER="${TD_USER:-dbc}"
TD_PASS="${TD_PASS:-dbc}"

export TD_HOME="${TD_HOME:-/opt/teradata/client/20.00}"
export PATH=$PATH:$TD_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TD_HOME/lib64:$TD_HOME/lib

echo -e "${GREEN}=== Teradata Table Operator Test ===${NC}"
echo "Host: $TD_HOST"
echo "User: $TD_USER"
echo ""

# Run test query via BTEQ
cat << EOF | bteq
.LOGON $TD_HOST/$TD_USER,$TD_PASS

DATABASE TrinoExport;

-- Verify Table Operator is registered
SELECT FunctionName, FunctionType, ParameterStyle 
FROM DBC.FunctionsV 
WHERE FunctionName LIKE 'EXPORT%';

-- Test with sample data
SELECT * FROM TestData;

-- Execute the Table Operator
SELECT * FROM ExportToTrino(
    ON (SELECT * FROM TestData)
) AS export_result;

.QUIT
EOF

echo ""
echo -e "${GREEN}=== Test Complete ===${NC}"
