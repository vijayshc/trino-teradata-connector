#!/bin/bash
# ============================================================
# Java Stress Test Runner - Concurrent Test Suites
# ============================================================

CONCURRENCY=${1:-3}
TEST_CLASS=${2:-""}

echo "=== Teradata-Trino Java Stress Test ==="
echo "Concurrency: $CONCURRENCY"
if [ -n "$TEST_CLASS" ]; then echo "Target Test: $TEST_CLASS"; fi

cd /home/vijay/tdconnector/tests/trino-tests
echo "Pre-compiling tests..."
mvn test-compile -DskipTests

mkdir -p stress_results_java

for i in $(seq 1 $CONCURRENCY); do
    (
        echo "Launching Instance $i..."
        # Use isolated project build directory to avoid race conditions in reports/classes
        if [ -n "$TEST_CLASS" ]; then
            mvn surefire:test -Dtest="$TEST_CLASS" -Dproject.build.directory="target_stress_$i" > "stress_results_java/inst_${i}.log" 2>&1
        else
            mvn surefire:test -Dproject.build.directory="target_stress_$i" > "stress_results_java/inst_${i}.log" 2>&1
        fi
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            echo "[SUCCESS] Instance $i finished."
        else
            echo "[FAILED] Instance $i failed with code $EXIT_CODE"
        fi
    ) &
done

# Monitor System
while [ $(jobs -r | wc -l) -gt 0 ]; do
    echo -n "Active Suites: $(jobs -r | wc -l) | "
    free -h | grep "Mem:" | awk '{print "Mem Available: " $7}'
    ps -Hu vijay | wc -l | xargs echo -n "Total threads: "
    echo ""
    sleep 5
done

wait
echo "=== Stress Test Complete ==="
# Cleanup temporary directories
rm -rf target_stress_*
