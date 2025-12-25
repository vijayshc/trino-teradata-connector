#!/bin/bash
set -e

# Build script for Trino Teradata Export Connector
# Requires: Maven, JDK 17+

cd src/trino

echo "Building Trino Connector using Maven..."
# Assuming mvn is in the path
if command -v mvn >/dev/null 2>&1; then
    mvn clean package -DskipTests
    echo "Success: JAR created in src/trino/target/"
else
    echo "Error: Maven (mvn) not found. Please install Maven to build the Trino plugin."
    exit 1
fi
