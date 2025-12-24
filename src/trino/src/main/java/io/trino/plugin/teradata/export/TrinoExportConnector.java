package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

public class TrinoExportConnector implements Connector {
    private static final Logger log = Logger.get(TrinoExportConnector.class);
    private final TrinoExportSplitManager splitManager;
    private final TrinoExportFlightServer flightServer;
    private final TeradataBridgeServer bridgeServer;
    private final TrinoExportMetadata metadata;
    private final TrinoExportConfig config;

    @Inject
    public TrinoExportConnector(
            TrinoExportSplitManager splitManager,
            TrinoExportFlightServer flightServer,
            TeradataBridgeServer bridgeServer,
            TrinoExportMetadata metadata,
            TrinoExportConfig config) {
        this.splitManager = splitManager;
        this.flightServer = flightServer;
        this.bridgeServer = bridgeServer;
        this.metadata = metadata;
        this.config = config;
        log.info("TrinoExportConnector instance created with integrated Java bridge");
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return (transaction, session, split, table, columns, dynamicFilter) -> {
            TrinoExportSplit exportSplit = (TrinoExportSplit) split;
            return new TrinoExportPageSource(exportSplit.getQueryId(), columns, config.getTeradataTimezone());
        };
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return TrinoExportTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        log.info("getMetadata called for query %s", session.getQueryId());
        return metadata;
    }

    @Override
    public void shutdown() {
        try {
            bridgeServer.close();
        } catch (Exception e) {
            log.warn("Error closing bridge server: %s", e.getMessage());
        }
        try {
            flightServer.close();
        } catch (Exception e) {
            log.warn("Error closing flight server: %s", e.getMessage());
        }
    }
}
