package io.trino.plugin.teradata.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

public class TrinoExportSplit implements ConnectorSplit {
    private final String workerHost;
    private final String queryId;
    private final String targetIps;

    @JsonCreator
    public TrinoExportSplit(
            @JsonProperty("workerHost") String workerHost,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("targetIps") String targetIps) {
        this.workerHost = workerHost;
        this.queryId = queryId;
        this.targetIps = targetIps;
    }

    @JsonProperty
    public String getWorkerHost() {
        return workerHost;
    }

    @JsonProperty
    public String getQueryId() {
        return queryId;
    }

    @JsonProperty
    public String getTargetIps() {
        return targetIps;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return List.of(HostAddress.fromString(workerHost));
    }

    public Map<String, String> getSplitInfo() {
        return Map.of(
            "workerHost", workerHost,
            "queryId", queryId,
            "targetIps", targetIps
        );
    }
}
