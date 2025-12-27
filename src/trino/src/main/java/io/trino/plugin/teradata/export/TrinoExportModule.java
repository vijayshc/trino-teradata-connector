package io.trino.plugin.teradata.export;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;

public class TrinoExportModule implements Module {
    @Override
    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(TrinoExportConfig.class);
        binder.bind(TrinoExportConnector.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TrinoExportFlightServer.class).in(Scopes.SINGLETON);
        binder.bind(TeradataBridgeServer.class).in(Scopes.SINGLETON);

    }
}
