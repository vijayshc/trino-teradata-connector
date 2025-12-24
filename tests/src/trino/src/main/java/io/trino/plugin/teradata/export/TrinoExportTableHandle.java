package io.trino.plugin.teradata.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Table handle that stores optimization information:
 * - projectedColumns: columns selected by Trino (column pruning)
 * - predicateClause: WHERE clause to push down to Teradata
 * - limit: LIMIT clause to push down to Teradata (using TOP N or SAMPLE)
 * - sortOrder: ORDER BY clause for Top-N pushdown (ORDER BY ... LIMIT N)
 * - aggregation: Aggregation pushdown information (aggregate functions + GROUP BY)
 */
public class TrinoExportTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;
    private final Optional<List<String>> projectedColumns;
    private final Optional<String> predicateClause;
    private final OptionalLong limit;
    private final Optional<List<SortItem>> sortOrder;
    private final Optional<AggregationInfo> aggregation;

    public TrinoExportTableHandle(SchemaTableName schemaTableName) {
        this(schemaTableName, Optional.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public TrinoExportTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("projectedColumns") Optional<List<String>> projectedColumns,
            @JsonProperty("predicateClause") Optional<String> predicateClause,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sortOrder") Optional<List<SortItem>> sortOrder,
            @JsonProperty("aggregation") Optional<AggregationInfo> aggregation) {
        this.schemaTableName = schemaTableName;
        this.projectedColumns = projectedColumns;
        this.predicateClause = predicateClause;
        this.limit = limit;
        this.sortOrder = sortOrder;
        this.aggregation = aggregation;
    }

    // Backward compatibility constructor (5 params)
    public TrinoExportTableHandle(
            SchemaTableName schemaTableName,
            Optional<List<String>> projectedColumns,
            Optional<String> predicateClause,
            OptionalLong limit,
            Optional<List<SortItem>> sortOrder) {
        this(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, Optional.empty());
    }

    // Backward compatibility constructor (4 params)
    public TrinoExportTableHandle(
            SchemaTableName schemaTableName,
            Optional<List<String>> projectedColumns,
            Optional<String> predicateClause,
            OptionalLong limit) {
        this(schemaTableName, projectedColumns, predicateClause, limit, Optional.empty(), Optional.empty());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public Optional<List<String>> getProjectedColumns() {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<String> getPredicateClause() {
        return predicateClause;
    }

    @JsonProperty
    public OptionalLong getLimit() {
        return limit;
    }

    @JsonProperty
    public Optional<List<SortItem>> getSortOrder() {
        return sortOrder;
    }

    @JsonProperty
    public Optional<AggregationInfo> getAggregation() {
        return aggregation;
    }

    public TrinoExportTableHandle withProjectedColumns(List<String> columns) {
        return new TrinoExportTableHandle(schemaTableName, Optional.of(columns), predicateClause, limit, sortOrder, aggregation);
    }

    public TrinoExportTableHandle withPredicateClause(String predicate) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, Optional.of(predicate), limit, sortOrder, aggregation);
    }

    public TrinoExportTableHandle withLimit(long limit) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, OptionalLong.of(limit), sortOrder, aggregation);
    }

    public TrinoExportTableHandle withSortOrder(List<SortItem> sortOrder) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, Optional.of(sortOrder), aggregation);
    }

    public TrinoExportTableHandle withTopN(List<SortItem> sortOrder, long limit) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, OptionalLong.of(limit), Optional.of(sortOrder), aggregation);
    }

    public TrinoExportTableHandle withAggregation(AggregationInfo aggregation) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, Optional.of(aggregation));
    }

    /**
     * Check if this is a Top-N query (both ORDER BY and LIMIT are present)
     */
    public boolean isTopN() {
        return sortOrder.isPresent() && !sortOrder.get().isEmpty() && limit.isPresent();
    }

    /**
     * Check if this query has aggregation pushdown
     */
    public boolean hasAggregation() {
        return aggregation.isPresent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrinoExportTableHandle that = (TrinoExportTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
               Objects.equals(projectedColumns, that.projectedColumns) &&
               Objects.equals(predicateClause, that.predicateClause) &&
               Objects.equals(limit, that.limit) &&
               Objects.equals(sortOrder, that.sortOrder) &&
               Objects.equals(aggregation, that.aggregation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, projectedColumns, predicateClause, limit, sortOrder, aggregation);
    }

    @Override
    public String toString() {
        String cols = projectedColumns.map(c -> c.toString()).orElse("*");
        String where = predicateClause.map(p -> " WHERE " + p).orElse("");
        String orderBy = sortOrder.map(s -> " ORDER BY " + SortItem.toSqlString(s)).orElse("");
        String limitStr = limit.isPresent() ? " LIMIT " + limit.getAsLong() : "";
        String aggStr = aggregation.map(a -> " [AGG: " + a.toString() + "]").orElse("");
        return schemaTableName.toString() + "[" + cols + "]" + where + orderBy + limitStr + aggStr;
    }

    /**
     * Represents a single column in an ORDER BY clause
     */
    public static class SortItem {
        private final String columnName;
        private final boolean ascending;
        private final boolean nullsFirst;

        @JsonCreator
        public SortItem(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("ascending") boolean ascending,
                @JsonProperty("nullsFirst") boolean nullsFirst) {
            this.columnName = columnName;
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
        }

        @JsonProperty
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty
        public boolean isAscending() {
            return ascending;
        }

        @JsonProperty
        public boolean isNullsFirst() {
            return nullsFirst;
        }

        public String toSql() {
            StringBuilder sb = new StringBuilder(columnName);
            sb.append(ascending ? " ASC" : " DESC");
            sb.append(nullsFirst ? " NULLS FIRST" : " NULLS LAST");
            return sb.toString();
        }

        public static String toSqlString(List<SortItem> items) {
            return items.stream()
                    .map(SortItem::toSql)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SortItem sortItem = (SortItem) o;
            return ascending == sortItem.ascending &&
                   nullsFirst == sortItem.nullsFirst &&
                   Objects.equals(columnName, sortItem.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, ascending, nullsFirst);
        }
    }

    /**
     * Stores aggregation pushdown information.
     * Contains the list of aggregate expressions and grouping columns.
     */
    public static class AggregationInfo {
        private final List<AggregateExpression> aggregates;
        private final List<String> groupByColumns;

        @JsonCreator
        public AggregationInfo(
                @JsonProperty("aggregates") List<AggregateExpression> aggregates,
                @JsonProperty("groupByColumns") List<String> groupByColumns) {
            this.aggregates = aggregates;
            this.groupByColumns = groupByColumns;
        }

        @JsonProperty
        public List<AggregateExpression> getAggregates() {
            return aggregates;
        }

        @JsonProperty
        public List<String> getGroupByColumns() {
            return groupByColumns;
        }

        /**
         * Generate the SELECT list for the aggregate query.
         */
        public String toSelectList() {
            StringBuilder sb = new StringBuilder();
            
            // First add GROUP BY columns
            for (int i = 0; i < groupByColumns.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(groupByColumns.get(i));
            }
            
            // Then add aggregate expressions
            for (int i = 0; i < aggregates.size(); i++) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(aggregates.get(i).toSql());
            }
            
            return sb.toString();
        }

        /**
         * Generate the SELECT list for the aggregate query, restricted to requested columns.
         */
        public String toSelectList(List<String> projectedColumns) {
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                return toSelectList();
            }
            
            StringBuilder sb = new StringBuilder();
            for (String colName : projectedColumns) {
                if (sb.length() > 0) sb.append(", ");
                
                // Check if it's a grouping column
                if (groupByColumns.contains(colName)) {
                    sb.append(colName);
                } else {
                    // Check if it's an aggregate synthetic column
                    boolean found = false;
                    for (AggregateExpression agg : aggregates) {
                        if (agg.getOutputAlias().equals(colName)) {
                            sb.append(agg.toSql());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // Fallback - just use the column name
                        sb.append(colName);
                    }
                }
            }
            return sb.toString();
        }

        /**
         * Generate the GROUP BY clause.
         */
        public String toGroupByClause() {
            if (groupByColumns.isEmpty()) {
                return "";
            }
            return " GROUP BY " + String.join(", ", groupByColumns);
        }

        @Override
        public String toString() {
            return toSelectList() + toGroupByClause();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AggregationInfo that = (AggregationInfo) o;
            return Objects.equals(aggregates, that.aggregates) &&
                   Objects.equals(groupByColumns, that.groupByColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregates, groupByColumns);
        }
    }

    /**
     * Represents a single aggregate expression like COUNT(*), SUM(amount), etc.
     */
    public static class AggregateExpression {
        private final String functionName;      // COUNT, SUM, MIN, MAX, AVG
        private final String columnName;        // Column to aggregate, or "*" for COUNT(*)
        private final String outputAlias;       // Alias for the result column
        private final boolean distinct;         // For COUNT(DISTINCT col)

        @JsonCreator
        public AggregateExpression(
                @JsonProperty("functionName") String functionName,
                @JsonProperty("columnName") String columnName,
                @JsonProperty("outputAlias") String outputAlias,
                @JsonProperty("distinct") boolean distinct) {
            this.functionName = functionName;
            this.columnName = columnName;
            this.outputAlias = outputAlias;
            this.distinct = distinct;
        }

        @JsonProperty
        public String getFunctionName() {
            return functionName;
        }

        @JsonProperty
        public String getColumnName() {
            return columnName;
        }

        @JsonProperty
        public String getOutputAlias() {
            return outputAlias;
        }

        @JsonProperty
        public boolean isDistinct() {
            return distinct;
        }

        public String toSql() {
            String col = columnName != null ? columnName : "*";
            String distinctStr = distinct ? "DISTINCT " : "";
            return functionName + "(" + distinctStr + col + ") AS " + outputAlias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AggregateExpression that = (AggregateExpression) o;
            return distinct == that.distinct &&
                   Objects.equals(functionName, that.functionName) &&
                   Objects.equals(columnName, that.columnName) &&
                   Objects.equals(outputAlias, that.outputAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(functionName, columnName, outputAlias, distinct);
        }
    }
}
