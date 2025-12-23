package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class TrinoExportMetadata implements io.trino.spi.connector.ConnectorMetadata {
    private static final Logger log = Logger.get(TrinoExportMetadata.class);
    private final TrinoExportConfig config;
    private final Map<io.trino.spi.connector.SchemaTableName, List<io.trino.spi.connector.ColumnMetadata>> tableCache = new ConcurrentHashMap<>();
    private Driver teradataDriver;

    @Inject
    public TrinoExportMetadata(TrinoExportConfig config) {
        this.config = config;
    }

    @Override
    public List<String> listSchemaNames(io.trino.spi.connector.ConnectorSession session) {
        log.info("Listing schema names from Teradata...");
        List<String> schemas = new ArrayList<>();
        try (Connection conn = getConnection()) {
            // Try catalogs first (databases in Teradata)
            try (ResultSet rs = conn.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    String cat = rs.getString("TABLE_CAT");
                    if (cat != null && !cat.isEmpty()) {
                        schemas.add(cat);
                    }
                }
            }
            
            // Then try schemas
            try (ResultSet rs = conn.getMetaData().getSchemas()) {
                while (rs.next()) {
                    String schem = rs.getString("TABLE_SCHEM");
                    if (schem != null && !schem.isEmpty() && !schemas.contains(schem)) {
                        schemas.add(schem);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, "Error listing schema names from Teradata");
        }
        
        if (!schemas.contains("TrinoExport")) {
            schemas.add("TrinoExport");
        }
        if (!schemas.contains("default")) {
            schemas.add("default");
        }
        log.info("Schemas found: %s", schemas);
        return schemas;
    }

    @Override
    public io.trino.spi.connector.ConnectorTableHandle getTableHandle(
            io.trino.spi.connector.ConnectorSession session, 
            io.trino.spi.connector.SchemaTableName tableName,
            Optional<io.trino.spi.connector.ConnectorTableVersion> startVersion,
            Optional<io.trino.spi.connector.ConnectorTableVersion> endVersion) {
        log.info("getTableHandle: %s", tableName);
        return new TrinoExportTableHandle(tableName);
    }

    @Override
    public io.trino.spi.connector.ConnectorTableMetadata getTableMetadata(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle table) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) table;
        return new io.trino.spi.connector.ConnectorTableMetadata(
                handle.getSchemaTableName(),
                getColumnMetadataList(handle.getSchemaTableName())
        );
    }

    private List<io.trino.spi.connector.ColumnMetadata> getColumnMetadataList(io.trino.spi.connector.SchemaTableName tableName) {
        return tableCache.computeIfAbsent(tableName, name -> {
            log.info("Fetching columns for %s (Schema: %s, Table: %s)", name, name.getSchemaName(), name.getTableName());
            List<io.trino.spi.connector.ColumnMetadata> columns = new ArrayList<>();
            
            try (Connection conn = getConnection()) {
                // Try as provided
                fetchColumns(conn, name.getSchemaName(), name.getTableName(), columns);
                
                // Try upper case
                if (columns.isEmpty()) {
                     fetchColumns(conn, name.getSchemaName(), name.getTableName().toUpperCase(), columns);
                }
                 // Try upper case schema
                if (columns.isEmpty()) {
                     fetchColumns(conn, name.getSchemaName().toUpperCase(), name.getTableName().toUpperCase(), columns);
                }

                log.info("Found %d columns for %s", columns.size(), name);
            } catch (Exception e) {
                log.error(e, "Error fetching columns for %s", name);
            }
            return columns;
        });
    }


    private void fetchColumns(Connection conn, String schema, String table, List<io.trino.spi.connector.ColumnMetadata> columns) throws Exception {
         // Try catalog-based (DatabaseName in Catalog)
         try (ResultSet rs = conn.getMetaData().getColumns(schema, null, table, null)) {
             while (rs.next()) {
                 String columnName = rs.getString("COLUMN_NAME");
                 int dataType = rs.getInt("DATA_TYPE");
                 int precision = rs.getInt("COLUMN_SIZE");
                 int scale = rs.getInt("DECIMAL_DIGITS");
                 columns.add(new io.trino.spi.connector.ColumnMetadata(columnName, mapJdbcTypeToTrino(dataType, precision, scale)));
             }
         }
         if (!columns.isEmpty()) return;

         // Try schema-based
         try (ResultSet rs = conn.getMetaData().getColumns(null, schema, table, null)) {
             while (rs.next()) {
                 String columnName = rs.getString("COLUMN_NAME");
                 int dataType = rs.getInt("DATA_TYPE");
                 int precision = rs.getInt("COLUMN_SIZE");
                 int scale = rs.getInt("DECIMAL_DIGITS");
                 columns.add(new io.trino.spi.connector.ColumnMetadata(columnName, mapJdbcTypeToTrino(dataType, precision, scale)));
             }
         }
    }

    @Override
    public List<io.trino.spi.connector.SchemaTableName> listTables(io.trino.spi.connector.ConnectorSession session, Optional<String> schemaName) {
        String schema = schemaName.orElse(null);
        log.info("Listing tables for schema %s", schema);
        List<io.trino.spi.connector.SchemaTableName> tables = new ArrayList<>();
        try (Connection conn = getConnection()) {
            // Try as catalog
            try (ResultSet rs = conn.getMetaData().getTables(schema, null, null, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    tables.add(new io.trino.spi.connector.SchemaTableName(schema, rs.getString("TABLE_NAME")));
                }
            }
            // Try as schema
            if (tables.isEmpty()) {
                try (ResultSet rs = conn.getMetaData().getTables(null, schema, null, new String[]{"TABLE", "VIEW"})) {
                    while (rs.next()) {
                        String realSchema = rs.getString("TABLE_SCHEM");
                        tables.add(new io.trino.spi.connector.SchemaTableName(realSchema != null ? realSchema : schema, rs.getString("TABLE_NAME")));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, "Error listing tables for schema %s", schema);
        }
        log.info("Found %d tables for schema %s", tables.size(), schema);
        return tables;
    }

    @Override
    public Map<String, io.trino.spi.connector.ColumnHandle> getColumnHandles(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle tableHandle) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) tableHandle;
        Map<String, io.trino.spi.connector.ColumnHandle> columnHandles = new ConcurrentHashMap<>();
        java.util.List<io.trino.spi.connector.ColumnMetadata> columns = getColumnMetadataList(handle.getSchemaTableName());
        for (int i = 0; i < columns.size(); i++) {
            io.trino.spi.connector.ColumnMetadata column = columns.get(i);
            columnHandles.put(column.getName(), new TrinoExportColumnHandle(column.getName(), column.getType(), i));
        }
        return columnHandles;
    }

    @Override
    public io.trino.spi.connector.ColumnMetadata getColumnMetadata(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle tableHandle, io.trino.spi.connector.ColumnHandle columnHandle) {
        TrinoExportColumnHandle handle = (TrinoExportColumnHandle) columnHandle;
        return new io.trino.spi.connector.ColumnMetadata(handle.getName(), handle.getType());
    }

    private synchronized Connection getConnection() throws Exception {
        if (teradataDriver == null) {
            log.info("Loading Teradata JDBC driver...");
            teradataDriver = (Driver) Class.forName("com.teradata.jdbc.TeraDriver").getDeclaredConstructor().newInstance();
        }
        Properties props = new Properties();
        props.setProperty("user", config.getTeradataUser());
        props.setProperty("password", config.getTeradataPassword());
        return teradataDriver.connect(config.getTeradataUrl(), props);
    }

    private io.trino.spi.type.Type mapJdbcTypeToTrino(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case java.sql.Types.INTEGER: return io.trino.spi.type.IntegerType.INTEGER;
            case java.sql.Types.BIGINT: return io.trino.spi.type.BigintType.BIGINT;
            
            case java.sql.Types.DATE: return io.trino.spi.type.DateType.DATE;
            // Map TIMESTAMP to VARCHAR because C UDF sends Hex String for now
            case java.sql.Types.TIMESTAMP: return io.trino.spi.type.VarcharType.VARCHAR;
            case java.sql.Types.TIME: return io.trino.spi.type.TimeType.TIME_MICROS;

            case java.sql.Types.REAL:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE: return io.trino.spi.type.DoubleType.DOUBLE;

            case java.sql.Types.TINYINT: return io.trino.spi.type.TinyintType.TINYINT;
            case java.sql.Types.SMALLINT: return io.trino.spi.type.SmallintType.SMALLINT;

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                // Cap precision at 38 for Trino
                if (precision > 38) precision = 38;
                if (precision <= 0) precision = 18;
                return io.trino.spi.type.DecimalType.createDecimalType(precision, scale);

            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR: return io.trino.spi.type.VarcharType.VARCHAR;
            default: return io.trino.spi.type.VarcharType.VARCHAR;
        }
    }
}
