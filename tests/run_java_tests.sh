#!/bin/bash
# =============================================================================
# Trino Connector Test Runner (Java-based)
# =============================================================================
# This script compiles and runs the Java test suite using the Trino JDBC driver.
#
# Usage:
#   ./tests/run_java_tests.sh              # Run all tests
#   ./tests/run_java_tests.sh basic        # Run only basic tests
#   ./tests/run_java_tests.sh datatype     # Run datatype tests
#   ./tests/run_java_tests.sh filter       # Run filter pushdown tests
#   ./tests/run_java_tests.sh null         # Run NULL handling tests
#   ./tests/run_java_tests.sh aggregation  # Run aggregation tests
#   ./tests/run_java_tests.sh projection   # Run projection tests
#   ./tests/run_java_tests.sh datetime     # Run datetime tests
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAVA_DIR="$SCRIPT_DIR/java"
TRINO_JDBC="$PROJECT_DIR/trino_server/trino-jdbc-479.jar"
JAVA_HOME="${JAVA_HOME:-$PROJECT_DIR/trino_server/jdk-25.0.1}"

# Check JDBC driver exists
if [ ! -f "$TRINO_JDBC" ]; then
    echo "ERROR: Trino JDBC driver not found at $TRINO_JDBC"
    exit 1
fi

# Compile
echo "Compiling test suite..."
cd "$JAVA_DIR"
"$JAVA_HOME/bin/javac" -cp "$TRINO_JDBC" TrinoConnectorTests.java

# Run
echo "Running tests..."
echo ""
"$JAVA_HOME/bin/java" -cp "$TRINO_JDBC:." TrinoConnectorTests "$@"
