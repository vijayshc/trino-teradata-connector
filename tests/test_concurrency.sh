#!/bin/bash
# =================================================================
# test_concurrency.sh - Test multiple concurrent queries to Trino
# =================================================================

TRINO_CLI="/home/vijay/tdconnector/trino_server/jdk-25.0.1/bin/java -jar /home/vijay/tdconnector/trino_server/trino-cli-479 --server http://localhost:8080"
QUERY="SELECT count(*) FROM tdexport.trinoexport.testdatae2e"

echo "Starting 5 concurrent queries..."

for i in {1..5}; do
    (
        START_TIME=$(date +%s%N)
        RESULT=$($TRINO_CLI --execute "$QUERY" --output-format CSV 2>&1)
        END_TIME=$(date +%s%N)
        DURATION=$(( (END_TIME - START_TIME) / 1000000 ))
        echo "Query $i finished in ${DURATION}ms. Result: $RESULT"
    ) &
done

wait
echo "All concurrent queries finished."
