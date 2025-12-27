package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.StandardErrorCode;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Collections;
import java.util.LinkedHashMap;

public class TrinoExportMetadata implements io.trino.spi.connector.ConnectorMetadata {
    private static final Logger log = Logger.get(TrinoExportMetadata.class);
    private final TrinoExportConfig config;
    
    // Bounded LRU table cache - max 1000 tables to prevent unbounded memory growth
    // Uses synchronized wrapper for thread safety
    private static final int MAX_CACHE_SIZE = 1000;
    private final Map<io.trino.spi.connector.SchemaTableName, List<io.trino.spi.connector.ColumnMetadata>> tableCache = 
            Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    boolean shouldRemove = size() > MAX_CACHE_SIZE;
                    if (shouldRemove) {
                        log.debug("Evicting oldest cache entry, cache size: %d", size());
                    }
                    return shouldRemove;
                }
            });

    @Inject
    public TrinoExportMetadata(TrinoExportConfig config) {
        this.config = config;
    }

    @Override
    public List<String> listSchemaNames(io.trino.spi.connector.ConnectorSession session) {
        log.info("Listing schema names from Teradata...");
        List<String> schemas = new ArrayList<>();
        try (Connection conn = getConnection(null)) {
            // Use schemas only (Databases in Teradata are mapped here)
            try (ResultSet rs = conn.getMetaData().getSchemas()) {
                while (rs.next()) {
                    String schem = rs.getString("TABLE_SCHEM");
                    if (schem != null && !schem.isEmpty() && !schemas.contains(schem)) {
                        schemas.add(schem);
                    }
                }
            }
        } catch (SQLException e) {
            log.error(e, "Teradata error listing schema names");
            throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, 
                    "Teradata error: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(e, "Error listing schema names from Teradata");
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, 
                    "Failed to list schemas: " + e.getMessage(), e);
        }
        
        // Add configurable default schemas
        for (String defaultSchema : config.getDefaultSchemasArray()) {
            String trimmed = defaultSchema.trim();
            if (!trimmed.isEmpty() && !schemas.contains(trimmed)) {
                schemas.add(trimmed);
            }
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
        
        // Validate that the table actually exists in Teradata before returning a handle
        // This prevents queries from proceeding to data transfer stage if table doesn't exist
        List<io.trino.spi.connector.ColumnMetadata> columns = getColumnMetadataList(session, tableName);
        if (columns == null || columns.isEmpty()) {
            log.warn("Table %s does not exist in Teradata or has no columns", tableName);
            // Return null to let Trino report standard "Table does not exist" error
            // This is the Trino convention for non-existent tables
            return null;
        }
        
        return new TrinoExportTableHandle(tableName);
    }

    @Override
    public io.trino.spi.connector.ConnectorTableMetadata getTableMetadata(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle table) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) table;
        return new io.trino.spi.connector.ConnectorTableMetadata(
                handle.getSchemaTableName(),
                getColumnMetadataList(session, handle.getSchemaTableName())
        );
    }

    private List<io.trino.spi.connector.ColumnMetadata> getColumnMetadataList(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.SchemaTableName tableName) {
        // Check bounded LRU cache first
        List<io.trino.spi.connector.ColumnMetadata> cached = tableCache.get(tableName);
        if (cached != null) {
            return cached;
        }
        
        log.info("Fetching columns for %s (Schema: %s, Table: %s)", tableName, tableName.getSchemaName(), tableName.getTableName());
        List<io.trino.spi.connector.ColumnMetadata> columns = new ArrayList<>();
        
        try (Connection conn = getConnection(null)) {
            // Try as provided
            fetchColumns(conn, tableName.getSchemaName(), tableName.getTableName(), columns);
            
            // Try upper case table name
            if (columns.isEmpty()) {
                 fetchColumns(conn, tableName.getSchemaName(), tableName.getTableName().toUpperCase(), columns);
            }
            // Try upper case schema and table
            if (columns.isEmpty()) {
                 fetchColumns(conn, tableName.getSchemaName().toUpperCase(), tableName.getTableName().toUpperCase(), columns);
            }

            log.info("Found %d columns for %s", columns.size(), tableName);
            
            // Cache the result (LRU eviction handles memory bounds)
            tableCache.put(tableName, columns);
            return columns;
            
        } catch (SQLException e) {
            // Propagate Teradata JDBC errors to Trino so user sees actual error message
            log.error(e, "Teradata error fetching columns for %s", tableName);
            throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, 
                    "Teradata error: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(e, "Error fetching columns for %s", tableName);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, 
                    "Failed to fetch table metadata: " + e.getMessage(), e);
        }
    }


    private void fetchColumns(Connection conn, String schema, String table, List<io.trino.spi.connector.ColumnMetadata> columns) throws Exception {
         // Use schema-based (TABLE_SCHEM in Teradata JDBC)
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
        try (Connection conn = getConnection(null)) {
            // Use schema-based only
            try (ResultSet rs = conn.getMetaData().getTables(null, schema, null, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String realSchema = rs.getString("TABLE_SCHEM");
                    tables.add(new io.trino.spi.connector.SchemaTableName(realSchema != null ? realSchema : schema, rs.getString("TABLE_NAME")));
                }
            }
        } catch (SQLException e) {
            log.error(e, "Teradata error listing tables for schema %s", schema);
            throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, 
                    "Teradata error: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(e, "Error listing tables for schema %s", schema);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, 
                    "Failed to list tables: " + e.getMessage(), e);
        }
        log.info("Found %d tables for schema %s", tables.size(), schema);
        return tables;
    }

    @Override
    public Map<String, io.trino.spi.connector.ColumnHandle> getColumnHandles(io.trino.spi.connector.ConnectorSession session, io.trino.spi.connector.ConnectorTableHandle tableHandle) {
        TrinoExportTableHandle handle = (TrinoExportTableHandle) tableHandle;
        Map<String, io.trino.spi.connector.ColumnHandle> columnHandles = new java.util.HashMap<>();
        java.util.List<io.trino.spi.connector.ColumnMetadata> columns = getColumnMetadataList(session, handle.getSchemaTableName());
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

    @Override
    public Optional<io.trino.spi.connector.ProjectionApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyProjection(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            List<io.trino.spi.expression.ConnectorExpression> projections,
            Map<String, io.trino.spi.connector.ColumnHandle> assignments) {
        
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) handle;
        
        // Extract base column names from the ColumnHandle values (not the aliased assignment keys)
        // Trino's optimizer assigns unique suffixes to column names (e.g., ss_sold_date_sk_359958)
        // but the ColumnHandle contains the original column name
        java.util.Set<String> baseColumnNames = new java.util.LinkedHashSet<>();
        for (io.trino.spi.connector.ColumnHandle colHandle : assignments.values()) {
            TrinoExportColumnHandle columnHandle = (TrinoExportColumnHandle) colHandle;
            baseColumnNames.add(columnHandle.getName());
        }
        List<String> projectedColumns = new ArrayList<>(baseColumnNames);
        
        // If no columns projected (e.g., SELECT 1), use all columns
        if (projectedColumns.isEmpty()) {
            return Optional.empty();
        }
        
        // Check if projection is already applied with same columns
        if (tableHandle.getProjectedColumns().isPresent() &&
            tableHandle.getProjectedColumns().get().equals(projectedColumns)) {
            log.debug("Projection already applied with same columns, skipping: %s", projectedColumns);
            return Optional.empty();
        }
        
        log.info("Applying column projection: %s", projectedColumns);
        
        // Create new table handle with projected columns
        TrinoExportTableHandle newHandle = tableHandle.withProjectedColumns(projectedColumns);
        
        // Build result assignments
        List<io.trino.spi.connector.Assignment> newAssignments = new ArrayList<>();
        
        for (Map.Entry<String, io.trino.spi.connector.ColumnHandle> entry : assignments.entrySet()) {
            TrinoExportColumnHandle columnHandle = (TrinoExportColumnHandle) entry.getValue();
            newAssignments.add(new io.trino.spi.connector.Assignment(
                    entry.getKey(),
                    columnHandle,
                    columnHandle.getType()
            ));
        }
        
        return Optional.of(new io.trino.spi.connector.ProjectionApplicationResult<>(
                newHandle,
                projections,
                newAssignments,
                false  // Not a partial projection
        ));
    }

    @Override
    public Optional<io.trino.spi.connector.ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyFilter(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            io.trino.spi.connector.Constraint constraint) {
        
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) handle;
        
        // Get the TupleDomain summary from the constraint
        io.trino.spi.predicate.TupleDomain<io.trino.spi.connector.ColumnHandle> summary = constraint.getSummary();
        
        // If no useful predicates, return empty
        if (summary.isAll() || summary.isNone()) {
            return Optional.empty();
        }
        
        // Check if we already have predicates applied
        if (tableHandle.getPredicateClause().isPresent()) {
            return Optional.empty();
        }
        
        // Try to extract predicates we can push down
        Optional<java.util.Map<io.trino.spi.connector.ColumnHandle, io.trino.spi.predicate.Domain>> domains = summary.getDomains();
        if (domains.isEmpty()) {
            return Optional.empty();
        }
        
        // Build SQL WHERE clause from domains
        List<String> predicateParts = new ArrayList<>();
        io.trino.spi.predicate.TupleDomain<io.trino.spi.connector.ColumnHandle> unenforcedDomain = 
                io.trino.spi.predicate.TupleDomain.all();
        
        for (java.util.Map.Entry<io.trino.spi.connector.ColumnHandle, io.trino.spi.predicate.Domain> entry : domains.get().entrySet()) {
            TrinoExportColumnHandle column = (TrinoExportColumnHandle) entry.getKey();
            io.trino.spi.predicate.Domain domain = entry.getValue();
            
            String sqlPredicate = TrinoExportFilterUtils.domainToSql(column.getName(), column.getType(), domain);
            if (sqlPredicate != null) {
                predicateParts.add(sqlPredicate);
            } else {
                // Could not push down, add to unenforced
                unenforcedDomain = unenforcedDomain.intersect(
                        io.trino.spi.predicate.TupleDomain.withColumnDomains(
                                java.util.Map.of(entry.getKey(), domain)));
            }
        }
        
        if (predicateParts.isEmpty()) {
            return Optional.empty();
        }
        
        String whereClause = String.join(" AND ", predicateParts);
        log.info("Applying filter pushdown: WHERE %s", whereClause);
        
        TrinoExportTableHandle newHandle = tableHandle.withPredicateClause(whereClause);
        
        return Optional.of(new io.trino.spi.connector.ConstraintApplicationResult<>(
                newHandle,
                unenforcedDomain,
                constraint.getExpression(),
                false  // Not exact predicate (Teradata may have different semantics)
        ));
    }

    @Override
    public Optional<io.trino.spi.connector.LimitApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyLimit(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            long limit) {
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) handle;

        if (tableHandle.getLimit().isPresent() && tableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        log.info("Applying limit pushdown: %d", limit);
        return Optional.of(new io.trino.spi.connector.LimitApplicationResult<>(
                tableHandle.withLimit(limit),
                true, // limit is guaranteed
                false)); // Not a projection pushdown
    }

    @Override
    public Optional<io.trino.spi.connector.TopNApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyTopN(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            long topNCount,
            List<io.trino.spi.connector.SortItem> sortItems,
            Map<String, io.trino.spi.connector.ColumnHandle> assignments) {
        
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) handle;
        
        // Check if Top-N pushdown is enabled in config
        if (!config.isEnableTopNPushdown()) {
            return Optional.empty();
        }
        
        // If already have a smaller Top-N, keep it
        if (tableHandle.isTopN() && tableHandle.getLimit().getAsLong() <= topNCount) {
            return Optional.empty();
        }
        
        // Convert Trino sort items to our format
        List<TrinoExportTableHandle.SortItem> sortOrder = new ArrayList<>();
        for (io.trino.spi.connector.SortItem item : sortItems) {
            TrinoExportColumnHandle columnHandle = (TrinoExportColumnHandle) assignments.get(item.getName());
            if (columnHandle == null) {
                // Can't find the column, give up on pushdown
                log.warn("Top-N pushdown: column not found in assignments: %s", item.getName());
                return Optional.empty();
            }
            
            boolean ascending = item.getSortOrder() == io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST ||
                               item.getSortOrder() == io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
            boolean nullsFirst = item.getSortOrder() == io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST ||
                                item.getSortOrder() == io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
            
            sortOrder.add(new TrinoExportTableHandle.SortItem(columnHandle.getName(), ascending, nullsFirst));
        }
        
        if (sortOrder.isEmpty()) {
            return Optional.empty();
        }
        
        log.info("Applying Top-N pushdown: ORDER BY %s LIMIT %d", 
                TrinoExportTableHandle.SortItem.toSqlString(sortOrder), topNCount);
        
        TrinoExportTableHandle newHandle = tableHandle.withTopN(sortOrder, topNCount);
        
        return Optional.of(new io.trino.spi.connector.TopNApplicationResult<>(
                newHandle,
                true,  // Top-N is guaranteed
                false  // Not a partial result
        ));
    }

    @Override
    public Optional<io.trino.spi.connector.AggregationApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> applyAggregation(
            io.trino.spi.connector.ConnectorSession session,
            io.trino.spi.connector.ConnectorTableHandle handle,
            List<io.trino.spi.connector.AggregateFunction> aggregates,
            Map<String, io.trino.spi.connector.ColumnHandle> assignments,
            List<List<io.trino.spi.connector.ColumnHandle>> groupingSets) {
        
        TrinoExportTableHandle tableHandle = (TrinoExportTableHandle) handle;
        
        // Check if aggregation pushdown is enabled in config
        if (!config.isEnableAggregationPushdown()) {
            return Optional.empty();
        }

        // Already have aggregation applied
        if (tableHandle.hasAggregation()) {
            return Optional.empty();
        }
        
        // Don't push aggregation if LIMIT or Top-N is already applied
        if (tableHandle.getLimit().isPresent()) {
            log.info("Aggregation pushdown: skipping because LIMIT is already present");
            return Optional.empty();
        }
        
        if (tableHandle.isTopN()) {
            log.info("Aggregation pushdown: skipping because Top-N is already present");
            return Optional.empty();
        }
        
        // Must have at least one aggregate
        if (aggregates.isEmpty()) {
            return Optional.empty();
        }
        
        // We only support simple grouping (single grouping set)
        if (groupingSets.size() != 1) {
            log.info("Aggregation pushdown: skipping ROLLUP/CUBE/GROUPING SETS (size=%d)", groupingSets.size());
            return Optional.empty();
        }
        
        List<io.trino.spi.connector.ColumnHandle> groupingColumns = groupingSets.get(0);
        
        // Convert grouping columns to names
        List<String> groupByColumnNames = new ArrayList<>();
        for (io.trino.spi.connector.ColumnHandle colHandle : groupingColumns) {
            TrinoExportColumnHandle column = (TrinoExportColumnHandle) colHandle;
            groupByColumnNames.add(column.getName());
        }
        
        // Build aggregate expressions and result columns
        List<TrinoExportTableHandle.AggregateExpression> aggregateExpressions = new ArrayList<>();
        List<io.trino.spi.expression.ConnectorExpression> projections = new ArrayList<>();
        List<io.trino.spi.connector.Assignment> resultAssignments = new ArrayList<>();
        Map<io.trino.spi.connector.ColumnHandle, io.trino.spi.connector.ColumnHandle> groupingColumnMapping = new java.util.HashMap<>();

        // Map grouping columns so Trino knows they are available.
        // They are NOT added to projections/assignments here; Trino handles them via the mapping.
        for (io.trino.spi.connector.ColumnHandle groupCol : groupingColumns) {
            groupingColumnMapping.put(groupCol, groupCol);
        }

        int nextSyntheticId = 0;
        for (io.trino.spi.connector.AggregateFunction aggregate : aggregates) {
            String functionName = aggregate.getFunctionName().toUpperCase();
            
            // Check if we support this function
            if (!isSupportedAggregateFunction(functionName)) {
                log.info("Aggregation pushdown: unsupported function %s", functionName);
                return Optional.empty();
            }
            
            // Check for unsupported features
            if (aggregate.getFilter().isPresent() || !aggregate.getSortItems().isEmpty()) {
                log.info("Aggregation pushdown: skipping aggregate with FILTER or ORDER BY");
                return Optional.empty();
            }
            
            // Skip DISTINCT aggregates for now - they're complex
            if (aggregate.isDistinct()) {
                log.info("Aggregation pushdown: skipping DISTINCT aggregate");
                return Optional.empty();
            }
            
            // Get the column being aggregated
            String columnName = null;
            io.trino.spi.type.Type inputType = null;
            
            if (!aggregate.getArguments().isEmpty()) {
                io.trino.spi.expression.ConnectorExpression input = aggregate.getArguments().get(0);
                if (input instanceof io.trino.spi.expression.Variable) {
                    String varName = ((io.trino.spi.expression.Variable) input).getName();
                    io.trino.spi.connector.ColumnHandle colHandle = assignments.get(varName);
                    if (colHandle != null) {
                        TrinoExportColumnHandle column = (TrinoExportColumnHandle) colHandle;
                        columnName = column.getName();
                        inputType = column.getType();
                    } else {
                        log.info("Aggregation pushdown: column not found for variable %s", varName);
                        return Optional.empty();
                    }
                } else {
                    log.info("Aggregation pushdown: complex expression not supported");
                    return Optional.empty();
                }
            }
            
            // Skip AVG and SUM on decimal types to avoid type mismatch
            if (inputType instanceof io.trino.spi.type.DecimalType && 
                (functionName.equals("AVG") || functionName.equals("SUM"))) {
                log.info("Aggregation pushdown: skipping %s on decimal type", functionName);
                return Optional.empty();
            }
            
            // Create synthetic column for aggregate result - use aggregate's declared output type
            String syntheticColumnName = "_agg_" + nextSyntheticId;
            nextSyntheticId++;
            
            io.trino.spi.type.Type outputType = aggregate.getOutputType();
            
            // Create the aggregate expression for Teradata SQL
            aggregateExpressions.add(new TrinoExportTableHandle.AggregateExpression(
                    functionName, columnName, syntheticColumnName, false));
            
            // Create column handle for the result
            TrinoExportColumnHandle resultColumn = new TrinoExportColumnHandle(
                    syntheticColumnName, outputType, nextSyntheticId);
            
            // Add projection (Variable pointing to synthetic column)
            projections.add(new io.trino.spi.expression.Variable(syntheticColumnName, outputType));
            
            // Add assignment
            resultAssignments.add(new io.trino.spi.connector.Assignment(
                    syntheticColumnName, resultColumn, outputType));
        }
        
        // Build aggregation info
        TrinoExportTableHandle.AggregationInfo aggregationInfo = 
                new TrinoExportTableHandle.AggregationInfo(aggregateExpressions, groupByColumnNames);
        
        log.info("Applying aggregation pushdown: %s", aggregationInfo.toString());
        
        TrinoExportTableHandle newHandle = tableHandle.withAggregation(aggregationInfo);
        
        // Return result with:
        // - projections: Variables for aggregate results ONLY
        // - resultAssignments: Assignments for aggregate results ONLY
        // - groupingColumnMapping: Empty (we use synthetic columns approach)
        return Optional.of(new io.trino.spi.connector.AggregationApplicationResult<>(
                newHandle,
                projections,
                resultAssignments,
                groupingColumnMapping,
                false));    // precalculateStatistics = false
    }
    
    private boolean isSupportedAggregateFunction(String functionName) {
        return switch (functionName) {
            case "COUNT", "SUM", "MIN", "MAX", "AVG" -> true;
            default -> false;
        };
    }
    
    private io.trino.spi.type.Type getAggregateOutputType(String functionName, io.trino.spi.type.Type inputType) {
        return switch (functionName) {
            case "COUNT" -> io.trino.spi.type.BigintType.BIGINT;
            case "SUM" -> {
                if (inputType instanceof io.trino.spi.type.DecimalType) {
                    // SUM of decimal returns a larger decimal
                    yield io.trino.spi.type.DecimalType.createDecimalType(38, 
                            ((io.trino.spi.type.DecimalType) inputType).getScale());
                }
                yield io.trino.spi.type.BigintType.BIGINT;
            }
            case "AVG" -> io.trino.spi.type.DoubleType.DOUBLE;
            case "MIN", "MAX" -> inputType != null ? inputType : io.trino.spi.type.BigintType.BIGINT;
            default -> io.trino.spi.type.BigintType.BIGINT;
        };
    }

    private Connection getConnection(String trinoUser) throws Exception {
        return TeradataConnectionFactory.getConnection(config, trinoUser);
    }

    private io.trino.spi.type.Type mapJdbcTypeToTrino(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case java.sql.Types.INTEGER: return io.trino.spi.type.IntegerType.INTEGER;
            case java.sql.Types.BIGINT: return io.trino.spi.type.BigintType.BIGINT;
            
            case java.sql.Types.DATE: return io.trino.spi.type.DateType.DATE;
            // Map TIMESTAMP to actual Trino Timestamp type (microseconds precision)
            case java.sql.Types.TIMESTAMP: return io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
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
