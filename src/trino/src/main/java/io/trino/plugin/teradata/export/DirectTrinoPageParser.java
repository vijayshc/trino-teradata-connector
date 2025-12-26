package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * High-performance direct parser that converts Teradata binary format directly to Trino Pages.
 * Option F: Skip Arrow Intermediary (Direct Parsing)
 * Option H: Parallel Column Conversion
 */
public final class DirectTrinoPageParser {
    private static final Logger log = Logger.get(DirectTrinoPageParser.class);

    // Thread pool for parallel column conversion
    private static final ExecutorService columnExecutor = Executors.newFixedThreadPool(
            Math.min(8, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "direct-parser-column");
                t.setDaemon(true);
                return t;
            }
    );

    public record ColumnSpec(String name, String type, Type trinoType) {}

    /**
     * Parse Teradata binary batch directly to Trino Page, bypassing Arrow.
     * This is much faster for simple types.
     */
    public static Page parseDirectToPage(byte[] data, int length, List<ColumnSpec> columns) {
        ByteBuffer buf = ByteBuffer.wrap(data, 0, length);
        int numRows = buf.getInt();

        if (numRows == 0) {
            return null;
        }

        int colCount = columns.size();
        Block[] blocks = new Block[colCount];

        // For small column counts, parse serially (thread overhead not worth it)
        if (colCount <= 4 || numRows < 1000) {
            for (int col = 0; col < colCount; col++) {
                blocks[col] = parseColumn(buf, numRows, columns.get(col));
            }
        } else {
            // Parallel column conversion for larger datasets
            // First, we need to read all row data into a column-oriented structure
            // This requires two passes but enables parallelism
            byte[][][] columnData = extractColumnData(buf, numRows, columns);
            
            CountDownLatch latch = new CountDownLatch(colCount);
            for (int col = 0; col < colCount; col++) {
                final int colIdx = col;
                columnExecutor.submit(() -> {
                    try {
                        blocks[colIdx] = buildBlock(columnData[colIdx], numRows, columns.get(colIdx));
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Column parsing interrupted", e);
            }
        }

        return new Page(numRows, blocks);
    }

    private static Block parseColumn(ByteBuffer buf, int numRows, ColumnSpec spec) {
        BlockBuilder builder = spec.trinoType.createBlockBuilder(null, numRows);
        String type = spec.type;

        for (int row = 0; row < numRows; row++) {
            byte isNull = buf.get();
            if (isNull == 1) {
                builder.appendNull();
                continue;
            }

            switch (type) {
                case "INTEGER", "DATE" -> INTEGER.writeLong(builder, buf.getInt());
                case "BIGINT", "TIME", "TIMESTAMP", "DECIMAL_SHORT" -> BIGINT.writeLong(builder, buf.getLong());
                case "DOUBLE" -> DOUBLE.writeDouble(builder, buf.getDouble());
                case "DECIMAL_LONG" -> {
                    byte[] bytes = new byte[16];
                    buf.get(bytes);
                    // Convert to Int128 for Trino decimal - use DecimalType directly
                    if (spec.trinoType instanceof DecimalType decimalType) {
                        decimalType.writeObject(builder, Int128.fromBigEndian(bytes));
                    } else {
                        builder.appendNull();
                    }
                }
                default -> {
                    int len = buf.getShort() & 0xFFFF;
                    byte[] strBytes = new byte[len];
                    buf.get(strBytes);
                    VARCHAR.writeSlice(builder, Slices.utf8Slice(new String(strBytes, StandardCharsets.UTF_8)));
                }
            }
        }

        return builder.build();
    }

    /**
     * Extract column data into a column-oriented structure for parallel processing.
     * Returns byte[col][row][data] where data includes null flag + value bytes.
     */
    private static byte[][][] extractColumnData(ByteBuffer buf, int numRows, List<ColumnSpec> columns) {
        int colCount = columns.size();
        byte[][][] result = new byte[colCount][numRows][];

        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < colCount; col++) {
                String type = columns.get(col).type;
                byte isNull = buf.get();

                if (isNull == 1) {
                    result[col][row] = new byte[]{1};
                } else {
                    int dataSize = switch (type) {
                        case "INTEGER", "DATE" -> 4;
                        case "BIGINT", "TIME", "TIMESTAMP", "DECIMAL_SHORT" -> 8;
                        case "DOUBLE" -> 8;
                        case "DECIMAL_LONG" -> 16;
                        default -> {
                            int len = buf.getShort() & 0xFFFF;
                            buf.position(buf.position() - 2);
                            yield 2 + len;
                        }
                    };

                    byte[] data = new byte[1 + dataSize];
                    data[0] = 0; // not null
                    buf.get(data, 1, dataSize);
                    result[col][row] = data;
                }
            }
        }

        return result;
    }

    private static Block buildBlock(byte[][] columnData, int numRows, ColumnSpec spec) {
        BlockBuilder builder = spec.trinoType.createBlockBuilder(null, numRows);
        String type = spec.type;

        for (int row = 0; row < numRows; row++) {
            byte[] data = columnData[row];
            if (data[0] == 1) {
                builder.appendNull();
                continue;
            }

            ByteBuffer buf = ByteBuffer.wrap(data, 1, data.length - 1);

            switch (type) {
                case "INTEGER", "DATE" -> INTEGER.writeLong(builder, buf.getInt());
                case "BIGINT", "TIME", "TIMESTAMP", "DECIMAL_SHORT" -> BIGINT.writeLong(builder, buf.getLong());
                case "DOUBLE" -> DOUBLE.writeDouble(builder, buf.getDouble());
                case "DECIMAL_LONG" -> {
                    byte[] bytes = new byte[16];
                    buf.get(bytes);
                    if (spec.trinoType instanceof DecimalType decimalType) {
                        decimalType.writeObject(builder, Int128.fromBigEndian(bytes));
                    } else {
                        builder.appendNull();
                    }
                }
                default -> {
                    int len = buf.getShort() & 0xFFFF;
                    byte[] strBytes = new byte[len];
                    buf.get(strBytes);
                    VARCHAR.writeSlice(builder, Slices.utf8Slice(new String(strBytes, StandardCharsets.UTF_8)));
                }
            }
        }

        return builder.build();
    }

    public static void shutdown() {
        columnExecutor.shutdown();
    }
}
