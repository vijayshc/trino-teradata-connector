package io.trino.plugin.teradata.export;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import io.trino.spi.connector.ColumnHandle;
import java.util.List;
import java.util.stream.Collectors;

public class TrinoExportPageSource implements ConnectorPageSource {
    private final String queryId;
    private final BlockingQueue<BatchContainer> buffer;
    private final List<TrinoExportColumnHandle> columns;
    private long completedBytes = 0;
    private boolean finished = false;

    public TrinoExportPageSource(String queryId, List<ColumnHandle> columns) {
        this.queryId = queryId;
        this.buffer = DataBufferRegistry.getBuffer(queryId);
        this.columns = columns.stream()
                .map(TrinoExportColumnHandle.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }

        try {
            BatchContainer container = buffer.poll(500, TimeUnit.MILLISECONDS);
            if (container == null) {
                return null;
            }

            if (container.isEndOfStream()) {
                io.airlift.log.Logger.get(TrinoExportPageSource.class).info("Received end of stream for query %s", queryId);
                finished = true;
                return null;
            }

            VectorSchemaRoot root = container.root();
            try {
                io.airlift.log.Logger.get(TrinoExportPageSource.class).info("Converting batch with %d rows for query %s", root.getRowCount(), queryId);
                Page page = convertToPage(root);
                completedBytes += page.getSizeInBytes();
                return SourcePage.create(page);
            } catch (Exception e) {
                io.airlift.log.Logger.get(TrinoExportPageSource.class).error(e, "Error converting batch for query %s", queryId);
                throw e;
            } finally {
                root.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finished = true;
            return null;
        }
    }

    private Page convertToPage(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        
        // Handle count(*) or no columns projected
        if (columns.isEmpty()) {
            return new Page(rowCount);
        }

        Block[] blocks = new Block[columns.size()];
        
        for (int i = 0; i < columns.size(); i++) {
            TrinoExportColumnHandle column = columns.get(i);
            int ordinal = column.getOrdinal();
            
            // Safety check
            if (ordinal >= root.getFieldVectors().size()) {
                throw new RuntimeException("Column ordinal " + ordinal + " is out of bounds for Arrow batch with " + root.getFieldVectors().size() + " vectors");
            }
            

            var vector = root.getVector(ordinal);
            io.trino.spi.type.Type type = column.getType();

            // Handle direct mapping for INT/BIGINT if vectors match
            if (type == io.trino.spi.type.IntegerType.INTEGER && vector instanceof IntVector intVector) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (intVector.isNull(j)) {
                        builder.appendNull();
                    } else {
                        type.writeLong(builder, intVector.get(j));
                    }
                }
                blocks[i] = builder.build();
            } else if (type == io.trino.spi.type.BigintType.BIGINT && vector instanceof BigIntVector bigIntVector) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (bigIntVector.isNull(j)) {
                        builder.appendNull();
                    } else {
                        type.writeLong(builder, bigIntVector.get(j));
                    }
                }
                blocks[i] = builder.build();
            } else if (type == io.trino.spi.type.DoubleType.DOUBLE && vector instanceof Float8Vector float8Vector) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (float8Vector.isNull(j)) {
                        builder.appendNull();
                    } else {
                        type.writeDouble(builder, float8Vector.get(j));
                    }
                }
                blocks[i] = builder.build();
            } else if (blockBuilderHandlesString(type) && vector instanceof VarCharVector varchars) {
                // Parse string to target type (DECIMAL, DATE, TIMESTAMP, or VARCHAR)
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (varchars.isNull(j)) {
                        builder.appendNull();
                    } else {
                        String strVal = new String(varchars.get(j), java.nio.charset.StandardCharsets.UTF_8);
                        writeStringValueToBlock(type, builder, strVal);
                    }
                }
                blocks[i] = builder.build();
            } else {
                // Fallback: convert whatever we have to type (if simple) or error
                // For now, treat as string fallback useful for debugging or loose typing
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (vector.isNull(j)) {
                        builder.appendNull();
                    } else {
                        Object value = vector.getObject(j);
                        String strValue = value != null ? value.toString() : "";
                         // Try to write as standard type if possible, or just fail for complex mismatches not handled above
                         // But for generic robustness, if target is VARCHAR, we write string
                         if (type instanceof VarcharType) {
                            VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(strValue));
                         } else {
                            // Try parsing string to target type
                            writeStringValueToBlock(type, builder, strValue);
                         }
                    }
                }
                blocks[i] = builder.build();
            }
        }
        
        return new Page(rowCount, blocks);
    }

    private boolean blockBuilderHandlesString(io.trino.spi.type.Type type) {
        return type instanceof io.trino.spi.type.DecimalType 
            || type instanceof io.trino.spi.type.DateType 
            || type instanceof io.trino.spi.type.TimestampType
            || type instanceof io.trino.spi.type.TimeType
            || type instanceof VarcharType
            || type == io.trino.spi.type.TinyintType.TINYINT
            || type == io.trino.spi.type.SmallintType.SMALLINT;
    }

    private void writeStringValueToBlock(io.trino.spi.type.Type type, BlockBuilder builder, String value) {
        try {
            if (type instanceof io.trino.spi.type.DecimalType decimalType) {
                java.math.BigDecimal bd = new java.math.BigDecimal(value);
                bd = bd.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP);
                if (decimalType.isShort()) {
                    type.writeLong(builder, bd.unscaledValue().longValueExact());
                } else {
                    type.writeObject(builder, io.trino.spi.type.Int128.valueOf(bd.unscaledValue()));
                }
            } else if (type instanceof io.trino.spi.type.DateType) {
                java.time.LocalDate date = java.time.LocalDate.parse(value);
                type.writeLong(builder, date.toEpochDay());
            } else if (type instanceof io.trino.spi.type.TimeType) {
                // Parse HH:MM:SS.ssssss
                java.time.LocalTime time = java.time.LocalTime.parse(value);
                // TimeType in Trino is pico seconds? or nano?
                // Default precision is usually 3 or 6.
                // Assuming standard TimeType (picos).
                // Wait, Trino TimeType writes long picos_of_day.
                long picos = time.toNanoOfDay() * 1000;
                type.writeLong(builder, picos);
            } else if (type instanceof io.trino.spi.type.TimestampType) {
                java.time.LocalDateTime dt = java.time.LocalDateTime.parse(value.replace(' ', 'T')); 
                long epochMicros = dt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli() * 1000 + (dt.getNano() / 1000) % 1000;
                 type.writeLong(builder, epochMicros); 
            } else if (type instanceof VarcharType) {
                VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value));
            } else if (type == io.trino.spi.type.IntegerType.INTEGER) {
                 type.writeLong(builder, Integer.parseInt(value));
            } else if (type == io.trino.spi.type.BigintType.BIGINT) {
                 type.writeLong(builder, Long.parseLong(value));
            } else if (type == io.trino.spi.type.TinyintType.TINYINT) {
                 type.writeLong(builder, Byte.parseByte(value));
            } else if (type == io.trino.spi.type.SmallintType.SMALLINT) {
                 type.writeLong(builder, Short.parseShort(value));
            } else {
                throw new RuntimeException("Unsupported type conversion from String to " + type);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert value '" + value + "' to type " + type, e);
        }
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        DataBufferRegistry.deregisterQuery(queryId);
    }
}
