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
 * - limit: LIMIT clause to push down to Teradata (using SAMPLE)
 */
public class TrinoExportTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;
    private final Optional<List<String>> projectedColumns;
    private final Optional<String> predicateClause;
    private final OptionalLong limit;

    public TrinoExportTableHandle(SchemaTableName schemaTableName) {
        this(schemaTableName, Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    @JsonCreator
    public TrinoExportTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("projectedColumns") Optional<List<String>> projectedColumns,
            @JsonProperty("predicateClause") Optional<String> predicateClause,
            @JsonProperty("limit") OptionalLong limit) {
        this.schemaTableName = schemaTableName;
        this.projectedColumns = projectedColumns;
        this.predicateClause = predicateClause;
        this.limit = limit;
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

    public TrinoExportTableHandle withProjectedColumns(List<String> columns) {
        return new TrinoExportTableHandle(schemaTableName, Optional.of(columns), predicateClause, limit);
    }

    public TrinoExportTableHandle withPredicateClause(String predicate) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, Optional.of(predicate), limit);
    }

    public TrinoExportTableHandle withLimit(long limit) {
        return new TrinoExportTableHandle(schemaTableName, projectedColumns, predicateClause, OptionalLong.of(limit));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrinoExportTableHandle that = (TrinoExportTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
               Objects.equals(projectedColumns, that.projectedColumns) &&
               Objects.equals(predicateClause, that.predicateClause) &&
               Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, projectedColumns, predicateClause, limit);
    }

    @Override
    public String toString() {
        String cols = projectedColumns.map(c -> c.toString()).orElse("*");
        String where = predicateClause.map(p -> " WHERE " + p).orElse("");
        String limitStr = limit.isPresent() ? " LIMIT " + limit.getAsLong() : "";
        return schemaTableName.toString() + "[" + cols + "]" + where + limitStr;
    }
}
