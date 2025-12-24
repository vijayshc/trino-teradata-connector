#!/bin/bash
# ============================================================
# Modular Trino Teradata Export Java Test Runner
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_PROJECT_DIR="$SCRIPT_DIR/trino-tests"
JAVA_HOME="/home/vijay/tdconnector/trino_server/jdk-25.0.1"
export PATH=$JAVA_HOME/bin:$PATH

cd "$TEST_PROJECT_DIR"

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "Usage: $0 [test-class-name]"
    echo "Example: $0 io.trino.tests.tdexport.BasicConnectivityTest"
    echo "Run all: $0"
    exit 0
fi

if [ -n "$1" ]; then
    echo "Running specific test: $1"
    mvn test -Dtest="$1"
else
    echo "Running full modular test suite..."
    mvn test
fi
