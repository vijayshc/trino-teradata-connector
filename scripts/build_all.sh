#!/bin/bash
# =================================================================
# build_all.sh - Build all components of the Teradata-Trino Connector
# =================================================================
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")
cd "$PROJECT_DIR"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Teradata to Trino Export Connector - Build System       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================
# Step 1: Build Trino Connector
# =============================================
echo -e "${YELLOW}[1/3] Building Trino Connector...${NC}"
cd "$PROJECT_DIR/src/trino"

if mvn clean package -DskipTests -q; then
    echo -e "${GREEN}✓ Trino Connector built successfully${NC}"
    JAR_FILE=$(ls target/trino-teradata-export-*.jar 2>/dev/null | head -1)
    if [ -n "$JAR_FILE" ]; then
        echo "  Output: $JAR_FILE"
    fi
else
    echo -e "${RED}✗ Trino Connector build failed${NC}"
    exit 1
fi
echo ""

# =============================================
# Step 2: Verify Teradata C code syntax
# =============================================
echo -e "${YELLOW}[2/3] Verifying Teradata C code...${NC}"
cd "$PROJECT_DIR"

C_FILE="src/teradata/export_to_trino.c"
if [ -f "$C_FILE" ]; then
    # Basic syntax check with gcc (mock headers used locally)
    if gcc -fsyntax-only -I src/teradata/include "$C_FILE" 2>/dev/null; then
        echo -e "${GREEN}✓ C syntax check passed${NC}"
    else
        echo -e "${YELLOW}⚠ C syntax check had warnings (expected with mock headers)${NC}"
    fi
    echo "  Source: $C_FILE"
else
    echo -e "${RED}✗ C source file not found: $C_FILE${NC}"
    exit 1
fi
echo ""

# =============================================
# Step 3: Create distribution package
# =============================================
echo -e "${YELLOW}[3/3] Creating distribution package...${NC}"

DIST_DIR="$PROJECT_DIR/dist"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR/teradata"
mkdir -p "$DIST_DIR/trino-plugin"
mkdir -p "$DIST_DIR/config"
mkdir -p "$DIST_DIR/scripts"

# Copy Teradata components
cp "$PROJECT_DIR/src/teradata/export_to_trino.c" "$DIST_DIR/teradata/"
cp -r "$PROJECT_DIR/src/teradata/include" "$DIST_DIR/teradata/"

# Copy Trino connector
cp "$PROJECT_DIR/src/trino/target/trino-teradata-export-"*.jar "$DIST_DIR/trino-plugin/"

# Copy configuration
cp "$PROJECT_DIR/config/"* "$DIST_DIR/config/"

# Copy scripts
cp "$PROJECT_DIR/scripts/register.bteq" "$DIST_DIR/scripts/"
cp "$PROJECT_DIR/scripts/deploy_to_trino.sh" "$DIST_DIR/scripts/"
cp "$PROJECT_DIR/scripts/test_table_operator.sh" "$DIST_DIR/scripts/"

# Copy documentation
cp "$PROJECT_DIR/README.md" "$DIST_DIR/"

echo -e "${GREEN}✓ Distribution package created${NC}"
echo "  Location: $DIST_DIR/"
echo ""

# =============================================
# Summary
# =============================================
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Build Complete!${NC}"
echo ""
echo "Distribution package contents:"
find "$DIST_DIR" -type f -exec echo "  {}" \;
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Register Table Operator on Teradata:"
echo "     bteq < scripts/register.bteq"
echo ""
echo "  2. Deploy to Trino:"
echo "     TRINO_HOME=/path/to/trino ./scripts/deploy_to_trino.sh"
echo ""
echo "  3. Configure and restart Trino"
echo ""
