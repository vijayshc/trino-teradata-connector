#!/bin/bash

# Configuration
SOURCE_DIR="/home/vijay/tdconnector"
TARGET_DIR="/home/vijay/tdconnector/opensource"
PLUGIN_DIR="/home/vijay/tdconnector/trino_server/trino-server-479/plugin/teradata-export"

echo "=== Syncing Project to Open Source Directory ==="

# 1. Create directory structure
mkdir -p "$TARGET_DIR/src/teradata"
mkdir -p "$TARGET_DIR/src/trino"
mkdir -p "$TARGET_DIR/docs"
mkdir -p "$TARGET_DIR/config"
mkdir -p "$TARGET_DIR/scripts"
mkdir -p "$TARGET_DIR/tests"
mkdir -p "$TARGET_DIR/target"
mkdir -p "$TARGET_DIR/plugin/teradata-export"

# 2. Copy Source Code
echo "Syncing source code..."
cp -r "$SOURCE_DIR/src/teradata/"* "$TARGET_DIR/src/teradata/"
cp -r "$SOURCE_DIR/src/trino/src" "$TARGET_DIR/src/trino/"
cp "$SOURCE_DIR/src/trino/pom.xml" "$TARGET_DIR/src/trino/"

# 3. Copy Documentation
echo "Syncing documentation..."
cp -r "$SOURCE_DIR/docs/"* "$TARGET_DIR/docs/"
cp "$SOURCE_DIR/README.md" "$TARGET_DIR/"
cp "$SOURCE_DIR/ENVIRONMENT_SETUP.md" "$TARGET_DIR/" 2>/dev/null || true
cp "$SOURCE_DIR/TECHNICAL_GUIDE_TDEXPORT.md" "$TARGET_DIR/" 2>/dev/null || true

# 4. Copy Configurations
echo "Syncing configurations..."
cp -r "$SOURCE_DIR/config/"* "$TARGET_DIR/config/"

# 5. Copy Build/Deploy Scripts
echo "Syncing scripts..."
cp -r "$SOURCE_DIR/scripts/"* "$TARGET_DIR/scripts/"

# 6. Copy Tests
echo "Syncing tests..."
cp -r "$SOURCE_DIR/tests/"* "$TARGET_DIR/tests/"

# 7. Copy Built JAR
echo "Syncing built artifact..."
JAR_FILE=$(ls "$SOURCE_DIR/src/trino/target/trino-teradata-export-"*".jar" 2>/dev/null | head -n 1)
if [ -n "$JAR_FILE" ]; then
    cp "$JAR_FILE" "$TARGET_DIR/target/trino-teradata-export.jar"
    cp $PLUGIN_DIR/* $TARGET_DIR/plugin/teradata-export/
    echo "Copied JAR: $JAR_FILE"
else
    echo "Warning: Built JAR not found in $SOURCE_DIR/src/trino/target/"
fi

# Cleanup: Remove any temporary, sensitive, or build-related files
find "$TARGET_DIR" -name "*.log" -delete
find "$TARGET_DIR" -name ".DS_Store" -delete
find "$TARGET_DIR" -name "*.class" -delete
find "$TARGET_DIR" -name "*.o" -delete
find "$TARGET_DIR" -type d -name "target" -not -path "$TARGET_DIR/target" -exec rm -rf {} +

echo "=== Sync Complete ==="
