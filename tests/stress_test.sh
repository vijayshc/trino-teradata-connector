#!/bin/bash
# ============================================================
# Stress Test Runner - Simulate high concurrency on a single node
# ============================================================
# Usage: ./stress_test.sh [concurrency_count] [iterations]
# ============================================================

CONCURRENCY=${1:-10}
ITERATIONS=${2:-1}
QUERY="SELECT * FROM tdexport.TrinoExport.test_unicode LIMIT 10000"

echo "=== Teradata-Trino Stress Test ==="
echo "Concurrency: $CONCURRENCY"
echo "Iterations: $ITERATIONS"
echo "Query: $QUERY"
echo "System Stats Before:"
free -h | grep "Mem:"
ps -Hu vijay | wc -l | xargs echo "Total threads for user vijay:"

mkdir -p stress_results

for i in $(seq 1 $ITERATIONS); do
    echo "--- Iteration $i ---"
    for j in $(seq 1 $CONCURRENCY); do
        (
            START=$(date +%s%3N)
            ./tests/quick_test.sh "$QUERY" > stress_results/q_${i}_${j}.log 2>&1
            EXIT_CODE=$?
            END=$(date +%s%3N)
            DURATION=$((END - START))
            if [ $EXIT_CODE -eq 0 ]; then
                echo "[SUCCESS] Query $j finished in ${DURATION}ms"
            else
                echo "[FAILED] Query $j failed after ${DURATION}ms (Check stress_results/q_${i}_${j}.log)"
            fi
        ) &
    done
    
    # Monitor while running
    while [ $(jobs -r | wc -l) -gt 0 ]; do
        echo -n "Active Queries: $(jobs -r | wc -l) | "
        free -h | grep "Mem:" | awk '{print "Mem Available: " $7}'
        ps -Hu vijay | wc -l | xargs echo -n "Total threads: "
        echo ""
        sleep 2
    done
done

echo "=== Stress Test Complete ==="
echo "System Stats After:"
free -h | grep "Mem:"
ps -Hu vijay | wc -l | xargs echo "Total threads for user vijay:"
