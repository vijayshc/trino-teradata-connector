#!/bin/bash
set -e

# Build script for Teradata Table Operator
# Requires: g++, Apache Arrow SDK, Teradata C API headers

SRC_DIR="src/teradata"
BUILD_DIR="build/teradata"
TARGET_SO="teradata_export.so"

# Default paths - can be overridden by environment variables
TD_INCLUDE=${TD_INCLUDE:-"/opt/teradata/client/include"}
ARROW_INCLUDE=${ARROW_INCLUDE:-"/usr/local/include"}
ARROW_LIB=${ARROW_LIB:-"/usr/local/lib"}

mkdir -p $BUILD_DIR

echo "Compiling Teradata Table Operator..."
g++ -shared -fPIC -O3 \
    -I$TD_INCLUDE \
    -Isrc/teradata/include \
    -I$ARROW_INCLUDE \
    -I$SRC_DIR \
    $SRC_DIR/teradata_export.cpp \
    -L$ARROW_LIB \
    -larrow -larrow_flight \
    -o $BUILD_DIR/$TARGET_SO

echo "Success: $BUILD_DIR/$TARGET_SO created."
echo "Ready for deployment to Teradata via 'CREATE FUNCTION' or 'REPLACE FUNCTION'."
