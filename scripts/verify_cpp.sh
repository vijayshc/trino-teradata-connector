#!/bin/bash
# Local verification script using mock headers

g++ -fsyntax-only \
    -Isrc/teradata/include \
    -Isrc/teradata \
    src/teradata/teradata_export.cpp

if [ $? -eq 0 ]; then
    echo "C++ Syntax Verification: PASSED"
else
    echo "C++ Syntax Verification: FAILED"
    exit 1
fi
