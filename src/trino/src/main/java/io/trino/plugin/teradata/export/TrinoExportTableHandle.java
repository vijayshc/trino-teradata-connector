package io.trino.plugin.teradata.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

public class TrinoExportTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public TrinoExportTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName) {
        this.schemaTableName = schemaTableName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrinoExportTableHandle that = (TrinoExportTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString() {
        return schemaTableName.toString();
    }
}
