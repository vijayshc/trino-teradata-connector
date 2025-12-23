package io.trino.plugin.teradata.export;

import org.apache.arrow.vector.VectorSchemaRoot;

public record BatchContainer(VectorSchemaRoot root, boolean isEndOfStream) {
    public static BatchContainer endOfStream() {
        return new BatchContainer(null, true);
    }
    
    public static BatchContainer of(VectorSchemaRoot root) {
        return new BatchContainer(root, false);
    }
}
