package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
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
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import io.trino.spi.connector.ColumnHandle;
import java.util.List;
import java.util.stream.Collectors;

public class TrinoExportPageSource implements ConnectorPageSource {
    private static final Logger log = Logger.get(TrinoExportPageSource.class);
    
    private final String queryId;
    private final BlockingQueue<BatchContainer> buffer;
    private final List<TrinoExportColumnHandle> columns;
    private final java.time.ZoneOffset teradataZoneOffset;
    private final java.time.ZoneOffset localZoneOffset;
    private final long pagePollTimeoutMs;
    private final boolean enableDebugLogging;
    private long completedBytes = 0;
    private boolean finished = false;

    public TrinoExportPageSource(String queryId, List<ColumnHandle> columns, String teradataTimezone, 
                                  long pagePollTimeoutMs, boolean enableDebugLogging) {
        this.queryId = queryId;
        
        // CRITICAL: Register buffer on THIS worker (multi-worker support)
        // Each worker needs its own buffer registration since DataBufferRegistry is per-JVM
        DataBufferRegistry.registerQuery(queryId);
        this.buffer = DataBufferRegistry.getBuffer(queryId);
        
        this.columns = columns.stream()
                .map(TrinoExportColumnHandle.class::cast)
                .collect(Collectors.toList());
        this.pagePollTimeoutMs = pagePollTimeoutMs;
        this.enableDebugLogging = enableDebugLogging;
        
        // Parse Teradata timezone offset
        this.teradataZoneOffset = java.time.ZoneOffset.of(teradataTimezone);
        // Get local timezone offset
        this.localZoneOffset = java.time.ZoneId.systemDefault().getRules().getOffset(java.time.Instant.now());
        
        log.info("PageSource created for query %s on worker (buffer registered), pollTimeout=%dms", 
                queryId, pagePollTimeoutMs);
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
            BatchContainer container = buffer.poll(pagePollTimeoutMs, TimeUnit.MILLISECONDS);
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
        
        // Build a name-to-index map for the Arrow batch
        // This enables name-based column mapping which works with SQL aliases
        Map<String, Integer> arrowColumnIndex = new java.util.HashMap<>();
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            arrowColumnIndex.put(root.getVector(i).getName().toLowerCase(), i);
        }
        
        // Determine if we should use position mapping
        boolean usePositionMapping = root.getFieldVectors().size() == columns.size();
        
        for (int i = 0; i < columns.size(); i++) {
            TrinoExportColumnHandle column = columns.get(i);
            
            // Try name-based mapping first (works with aliased columns)
            Integer vectorIndex = arrowColumnIndex.get(column.getName().toLowerCase());
            
            if (vectorIndex == null) {
                // Fall back to position or ordinal
                if (usePositionMapping) {
                    vectorIndex = i;
                } else {
                    vectorIndex = column.getOrdinal();
                }
            }
            
            // Safety check
            if (vectorIndex >= root.getFieldVectors().size()) {
                throw new RuntimeException("Column '" + column.getName() + "' (index " + vectorIndex + ") is out of bounds for Arrow batch with " + root.getFieldVectors().size() + " vectors. Available: " + arrowColumnIndex.keySet());
            }

            var vector = root.getVector(vectorIndex);
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
            } else if (type instanceof io.trino.spi.type.DateType && vector instanceof IntVector intVector) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (intVector.isNull(j)) builder.appendNull();
                    else type.writeLong(builder, (long) intVector.get(j));
                }
                blocks[i] = builder.build();
            } else if (type instanceof io.trino.spi.type.DecimalType decimalType && vector instanceof BigIntVector bigIntVector && decimalType.isShort()) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (bigIntVector.isNull(j)) builder.appendNull();
                    else type.writeLong(builder, bigIntVector.get(j));
                }
                blocks[i] = builder.build();
            } else if (type instanceof io.trino.spi.type.DecimalType decimalType && vector instanceof FixedSizeBinaryVector binaryVector && !decimalType.isShort()) {
                BlockBuilder builder = type.createBlockBuilder(null, rowCount);
                for (int j = 0; j < rowCount; j++) {
                    if (binaryVector.isNull(j)) builder.appendNull();
                    else {
                        java.math.BigInteger unscaled = new java.math.BigInteger(reverse(binaryVector.get(j)));
                        type.writeObject(builder, io.trino.spi.type.Int128.valueOf(unscaled));
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
                        
                        // Debug logging for type conversion issues
                        if (j == 0 && enableDebugLogging) {
                            io.airlift.log.Logger.get(TrinoExportPageSource.class).info(
                                "Column %s: Target type=%s, Vector type=%s, Value class=%s, Raw value='%s'",
                                column.getName(), type, vector.getClass().getSimpleName(), 
                                value != null ? value.getClass().getSimpleName() : "null", strValue);
                        }
                        
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
                java.math.BigDecimal bd;
                // Check if value is hex-encoded binary (from write_hex_string fallback)
                // Hex format: uppercase letters A-F and digits, even length
                if (value.matches("[0-9A-F]+") && value.length() % 2 == 0 && value.length() <= 32) {
                    // Parse as hex-encoded little-endian binary decimal
                    bd = parseHexDecimal(value, decimalType.getScale());
                } else {
                    // Normal decimal string format (e.g., "33.99")
                    bd = new java.math.BigDecimal(value);
                }
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
                // Parse TIME value from Teradata
                // Teradata C code sends time as "HH:MM:SS.ffffff" or "HH:MM:SS"
                // Note: The C code may have timezone/encoding issues, so we parse flexibly
                long picos;
                try {
                    // Try standard parsing first
                    java.time.LocalTime time = parseTeradataTime(value);
                    picos = time.toNanoOfDay() * 1000;
                } catch (Exception e) {
                    log.debug("Could not parse time '%s', using fallback: %s", value, e.getMessage());
                    picos = 0;
                }
                type.writeLong(builder, picos);
            } else if (type instanceof io.trino.spi.type.TimestampType) {
                // Parse "YYYY-MM-DD HH:MM:SS.ssssss" (Teradata format)
                String isoValue = value.replace(' ', 'T');
                try {
                    java.time.LocalDateTime dt = java.time.LocalDateTime.parse(isoValue);
                    // Apply timezone correction (same as TIME)
                    dt = dt.plusSeconds(teradataZoneOffset.getTotalSeconds());
                    
                    long epochMicros = dt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000 + (dt.getNano() / 1000);
                    type.writeLong(builder, epochMicros);
                } catch (Exception e) {
                    log.warn("Failed to parse timestamp '%s': %s", value, e.getMessage());
                    type.writeLong(builder, 0);
                }
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

    /**
     * Parse hex-encoded binary decimal from Teradata.
     * Teradata sends decimals as little-endian binary when the C code doesn't recognize
     * the specific decimal type code and falls back to write_hex_string.
     * 
     * Example: "470D0000" for DECIMAL(7,2) 
     *   - little-endian bytes: 47 0D 00 00 -> 0x00000D47 = 3399
     *   - with scale 2: 33.99
     */
    private java.math.BigDecimal parseHexDecimal(String hexValue, int scale) {
        // Convert hex string to bytes (each pair of chars = 1 byte)
        int byteLen = hexValue.length() / 2;
        byte[] bytes = new byte[byteLen];
        for (int i = 0; i < byteLen; i++) {
            bytes[i] = (byte) Integer.parseInt(hexValue.substring(i * 2, i * 2 + 2), 16);
        }
        
        // Interpret as little-endian signed integer
        long rawValue = 0;
        for (int i = byteLen - 1; i >= 0; i--) {
            rawValue = (rawValue << 8) | (bytes[i] & 0xFF);
        }
        
        // Handle sign extension for negative numbers depending on byte size
        if (byteLen == 1 && (bytes[0] & 0x80) != 0) {
            rawValue |= 0xFFFFFFFFFFFFFF00L;
        } else if (byteLen == 2 && (bytes[1] & 0x80) != 0) {
            rawValue |= 0xFFFFFFFFFFFF0000L;
        } else if (byteLen == 4 && (bytes[3] & 0x80) != 0) {
            rawValue |= 0xFFFFFFFF00000000L;
        }
        // For 8-byte values, rawValue already has correct sign
        
        // Convert to BigDecimal with scale
        java.math.BigDecimal bd = java.math.BigDecimal.valueOf(rawValue);
        return bd.movePointLeft(scale);
    }

    /**
     * Parse TIME value from Teradata C code.
     * The C code sends time in format "HH:MM:SS.ffffff" but may have encoding issues
     * due to how Teradata stores TIME internally.
     * 
     * Teradata TIME internal format (6 bytes):
     *   - Bytes 0-3: Scaled seconds (microseconds)
     *   - Bytes 4-5: Hour and Minute (may have offset issues)
     * 
     * If the standard parsing fails, we try to extract components manually.
     */
    private java.time.LocalTime parseTeradataTime(String value) {
        try {
            // Handle format like "HH:MM:SS.ffffff" or "HH:MM:SS"
            java.time.LocalTime rawTime;
            
            if (value.contains(".")) {
                // Has fractional seconds - need to normalize
                String[] parts = value.split("\\.");
                String timePart = parts[0];
                String fracPart = parts[1];
                
                // Normalize fractional part to 9 digits for nanos
                while (fracPart.length() < 9) {
                    fracPart = fracPart + "0";
                }
                if (fracPart.length() > 9) {
                    fracPart = fracPart.substring(0, 9);
                }
                
                // Parse time part
                String[] timeComponents = timePart.split(":");
                int hour = Integer.parseInt(timeComponents[0]);
                int minute = Integer.parseInt(timeComponents[1]);
                int second = (int) Double.parseDouble(timeComponents[2]);
                int nanos = Integer.parseInt(fracPart);
                
                rawTime = java.time.LocalTime.of(hour % 24, minute, second, nanos);
            } else {
                // No fractional seconds
                rawTime = java.time.LocalTime.parse(value);
            }
            
            // Apply timezone correction:
            // The C code reads TIME with an offset due to how Teradata stores it internally.
            // We need to adjust from Teradata's perceived time to the correct local time.
            // 
            // The observed offset is the difference between Teradata TZ and the local TZ.
            // If Teradata is -05:00 and local is +08:00, the offset is +13 hours.
            // But the C code's binary parsing causes a different offset (-5 hours from stored).
            // 
            // To correct: we subtract the Teradata timezone offset from the raw time.
            // This converts from "Teradata local" to UTC, then Trino TIME is typically
            // interpreted as local time anyway.
            
            int teradataOffsetSeconds = teradataZoneOffset.getTotalSeconds();
            // Add the Teradata offset to get the "intended" time
            // Example: If rawTime is 04:59:59 (UTC) and teradataOffset is -05:00 (-18000 sec)
            // correctedTime = 04:59:59 + (-5 hours) = 23:59:59
            java.time.LocalTime correctedTime = rawTime.plusSeconds(teradataOffsetSeconds);
            
            return correctedTime;
            
        } catch (Exception e) {
            // If all else fails, return the raw parsed time
            log.warn("Failed to parse time value '%s': %s, using midnight", value, e.getMessage());
            return java.time.LocalTime.MIDNIGHT;
        }
    }

    private byte[] reverse(byte[] bytes) {
        byte[] reversed = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            reversed[i] = bytes[bytes.length - 1 - i];
        }
        return reversed;
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
