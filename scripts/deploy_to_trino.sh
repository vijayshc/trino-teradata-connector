#!/bin/bash
# =================================================================
# deploy_to_trino.sh - Deploy the connector to a Trino installation
# =================================================================
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")
PLUGIN_NAME="teradata-export"
JAR_NAME="trino-teradata-export-1.0-SNAPSHOT.jar"

# Check for Trino home
if [ -z "$TRINO_HOME" ]; then
    echo -e "${YELLOW}TRINO_HOME not set. Please set it to your Trino installation directory.${NC}"
    echo "Example: export TRINO_HOME=/opt/trino"
    exit 1
fi

PLUGIN_DIR="$TRINO_HOME/plugin/$PLUGIN_NAME"
CATALOG_DIR="$TRINO_HOME/etc/catalog"

echo -e "${GREEN}=== Teradata Export Connector Deployment ===${NC}"
echo "Project Directory: $PROJECT_DIR"
echo "Trino Home: $TRINO_HOME"
echo ""

# Build if needed
if [ ! -f "$PROJECT_DIR/src/trino/target/$JAR_NAME" ]; then
    echo -e "${YELLOW}Building connector...${NC}"
    cd "$PROJECT_DIR/src/trino"
    mvn package -DskipTests
fi

# Create plugin directory
echo -e "${YELLOW}Installing plugin...${NC}"
mkdir -p "$PLUGIN_DIR"

# Copy JAR and dependencies
cp "$PROJECT_DIR/src/trino/target/$JAR_NAME" "$PLUGIN_DIR/"

# Copy dependencies (Arrow, etc.)
if [ -d "$PROJECT_DIR/src/trino/target/dependency" ]; then
    cp "$PROJECT_DIR/src/trino/target/dependency/"*.jar "$PLUGIN_DIR/" 2>/dev/null || true
fi

echo -e "${GREEN}Plugin installed to: $PLUGIN_DIR${NC}"

# Copy catalog configuration
if [ ! -f "$CATALOG_DIR/tdexport.properties" ]; then
    echo -e "${YELLOW}Installing catalog configuration...${NC}"
    mkdir -p "$CATALOG_DIR"
    cp "$PROJECT_DIR/config/teradata-export.properties" "$CATALOG_DIR/tdexport.properties"
    echo -e "${GREEN}Catalog configuration installed as tdexport.properties.${NC}"
    echo -e "${YELLOW}NOTE: Edit $CATALOG_DIR/tdexport.properties with your Teradata credentials!${NC}"
else
    echo -e "${YELLOW}Catalog config exists. To update, manually copy:${NC}"
    echo "  cp $PROJECT_DIR/config/teradata-export.properties $CATALOG_DIR/tdexport.properties"
fi

echo ""
echo -e "${GREEN}=== Deployment Complete ===${NC}"
echo ""
echo "Next steps:"
echo "  1. Copy Teradata JDBC driver to $PLUGIN_DIR/"
echo "  2. Edit $CATALOG_DIR/tdexport.properties with your credentials"
echo "  3. Start the Arrow Bridge on each worker"
echo "  4. Register the UDF on Teradata using scripts/register.bteq"
echo "  5. Restart Trino: $TRINO_HOME/bin/launcher restart"
echo ""
echo "To test the connector, run:"
echo "  trino> SHOW CATALOGS;"
echo "  trino> USE tdexport.your_database;"
echo "  trino> SELECT * FROM your_table LIMIT 10;"
