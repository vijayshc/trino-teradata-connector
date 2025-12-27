#!/bin/bash
# ============================================================
# Teradata-Trino Connector Stress Test Runner
# ============================================================
# Usage:
#   ./run_stress_test.sh [test|analyze|escalate] [options]
#
# Commands:
#   test      - Run general stress test (default)
#   analyze   - Run bottleneck analyzer
#   escalate  - Run load escalation test
#
# Examples:
#   ./run_stress_test.sh                          # Default stress test
#   ./run_stress_test.sh test --concurrency 50   # 50 concurrent queries
#   ./run_stress_test.sh analyze                  # Full bottleneck analysis
#   ./run_stress_test.sh analyze connection       # Test only connection pool
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
TRINO_JDBC="/home/vijay/tdconnector/trino_server/trino-jdbc-479.jar"

# Set Java Home
export JAVA_HOME=/home/vijay/tdconnector/trino_server/jdk-25.0.1
export PATH=$JAVA_HOME/bin:$PATH

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cd "$PROJECT_DIR"

# Build if needed
if [ ! -d "target/classes" ] || [ "src/main/java/io/trino/stress/StressTestRunner.java" -nt "target/classes/io/trino/stress/StressTestRunner.class" ]; then
    echo -e "${YELLOW}Building stress test project...${NC}"
    mvn compile -q
fi

COMMAND=${1:-test}
shift 2>/dev/null || true

case "$COMMAND" in
    test)
        echo -e "${GREEN}Running Stress Test...${NC}"
        java -cp "target/classes:$TRINO_JDBC" io.trino.stress.StressTestRunner "$@"
        ;;
    analyze)
        echo -e "${GREEN}Running Bottleneck Analyzer...${NC}"
        java -cp "target/classes:$TRINO_JDBC" io.trino.stress.BottleneckAnalyzer "$@"
        ;;
    escalate)
        echo -e "${GREEN}Running Load Escalation Test...${NC}"
        java -cp "target/classes:$TRINO_JDBC" io.trino.stress.BottleneckAnalyzer escalation
        ;;
    quick)
        echo -e "${GREEN}Running Quick Smoke Test (10s, 5 threads)...${NC}"
        java -cp "target/classes:$TRINO_JDBC" io.trino.stress.StressTestRunner \
            --concurrency 5 --duration 10 --ramp-up 0 --query-type small
        ;;
    heavy)
        echo -e "${GREEN}Running Heavy Load Test (60s, 100 threads)...${NC}"
        java -cp "target/classes:$TRINO_JDBC" io.trino.stress.StressTestRunner \
            --concurrency 100 --duration 60 --ramp-up 10 --query-type mixed
        ;;
    *)
        echo "Usage: $0 [test|analyze|escalate|quick|heavy] [options]"
        echo ""
        echo "Commands:"
        echo "  test      - Run general stress test (default)"
        echo "  analyze   - Run bottleneck analyzer"
        echo "  escalate  - Run load escalation test"
        echo "  quick     - Quick smoke test (5 threads, 10 seconds)"
        echo "  heavy     - Heavy load test (100 threads, 60 seconds)"
        echo ""
        echo "Options for 'test' command:"
        echo "  --concurrency N   Number of concurrent queries (default: 10)"
        echo "  --duration N      Test duration in seconds (default: 60)"
        echo "  --ramp-up N       Ramp-up period in seconds (default: 5)"
        echo "  --query-type TYPE Query type: small|medium|large|mixed (default: mixed)"
        echo ""
        echo "Options for 'analyze' command:"
        echo "  connection   - Test connection pool limits only"
        echo "  threadpool   - Test thread pool saturation only"
        echo "  buffer       - Test buffer queue pressure only"
        echo "  serialize    - Test serialization overhead only"
        echo "  escalation   - Run load escalation test only"
        exit 1
        ;;
esac
