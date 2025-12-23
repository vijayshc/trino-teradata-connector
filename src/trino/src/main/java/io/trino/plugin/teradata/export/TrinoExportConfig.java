package io.trino.plugin.teradata.export;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class TrinoExportConfig {
    private int flightPort = 50051;
    private String teradataUrl;
    private String teradataUser;
    private String teradataPassword;
    private int bridgePort = 9999;
    private String trinoAddress = "172.27.251.157";

    public int getBridgePort() {
        return bridgePort;
    }

    @Config("teradata.export.bridge-port")
    public TrinoExportConfig setBridgePort(int bridgePort) {
        this.bridgePort = bridgePort;
        return this;
    }

    public String getTrinoAddress() {
        return trinoAddress;
    }

    @Config("teradata.export.trino-address")
    public TrinoExportConfig setTrinoAddress(String trinoAddress) {
        this.trinoAddress = trinoAddress;
        return this;
    }

    public int getFlightPort() {
        return flightPort;
    }

    @Config("teradata.export.flight-port")
    public TrinoExportConfig setFlightPort(int flightPort) {
        this.flightPort = flightPort;
        return this;
    }

    public String getTeradataUrl() {
        return teradataUrl;
    }

    @Config("teradata.url")
    public TrinoExportConfig setTeradataUrl(String teradataUrl) {
        this.teradataUrl = teradataUrl;
        return this;
    }

    public String getTeradataUser() {
        return teradataUser;
    }

    @Config("teradata.user")
    public TrinoExportConfig setTeradataUser(String teradataUser) {
        this.teradataUser = teradataUser;
        return this;
    }

    public String getTeradataPassword() {
        return teradataPassword;
    }

    @Config("teradata.password")
    @ConfigSecuritySensitive
    public TrinoExportConfig setTeradataPassword(String teradataPassword) {
        this.teradataPassword = teradataPassword;
        return this;
    }
}
