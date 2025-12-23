package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

public class TrinoExportConnector implements Connector {
    private static final Logger log = Logger.get(TrinoExportConnector.class);
    private final TrinoExportSplitManager splitManager;
    private final TrinoExportFlightServer flightServer;
    private final TrinoExportMetadata metadata;

    @Inject
    public TrinoExportConnector(
            TrinoExportSplitManager splitManager,
            TrinoExportFlightServer flightServer,
            TrinoExportMetadata metadata) {
        this.splitManager = splitManager;
        this.flightServer = flightServer;
        this.metadata = metadata;
        log.info("TrinoExportConnector instance created");
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return (transaction, session, split, table, columns, dynamicFilter) -> {
            TrinoExportSplit exportSplit = (TrinoExportSplit) split;
            return new TrinoExportPageSource(exportSplit.getQueryId(), columns);
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
            flightServer.close();
        } catch (Exception e) {
            // Ignore shutdown errors
        }
    }
}
