package io.trino.plugin.teradata.export;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import java.util.concurrent.TimeUnit;

public class TrinoExportConfig {
    private int flightPort = 50051;
    private String teradataUrl;
    private String teradataUser;
    private String teradataPassword;
    private String passwordScript;
    private int bridgePort = 9999;
    private String trinoAddress = "172.27.251.157";
    private Duration dynamicFilterTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean enableDynamicFiltering = true;
    private String teradataTimezone = "-05:00";
    private String securityToken;
    private String tokenScript;

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

    @Config("teradata.password")
    @ConfigSecuritySensitive
    public TrinoExportConfig setTeradataPassword(String teradataPassword) {
        this.teradataPassword = teradataPassword;
        return this;
    }

    @Config("teradata.password-script")
    public TrinoExportConfig setPasswordScript(String passwordScript) {
        this.passwordScript = passwordScript;
        return this;
    }

    public String getPasswordScript() {
        return passwordScript;
    }

    public String getTeradataPassword() {
        if (passwordScript != null && !passwordScript.isEmpty()) {
            return executeScript(passwordScript);
        }
        return teradataPassword;
    }

    public Duration getDynamicFilterTimeout() {
        return dynamicFilterTimeout;
    }

    @Config("teradata.export.dynamic-filter-timeout")
    public TrinoExportConfig setDynamicFilterTimeout(Duration dynamicFilterTimeout) {
        this.dynamicFilterTimeout = dynamicFilterTimeout;
        return this;
    }

    public boolean isEnableDynamicFiltering() {
        return enableDynamicFiltering;
    }

    @Config("teradata.export.enable-dynamic-filtering")
    public TrinoExportConfig setEnableDynamicFiltering(boolean enableDynamicFiltering) {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public String getTeradataTimezone() {
        return teradataTimezone;
    }

    @Config("teradata.timezone")
    public TrinoExportConfig setTeradataTimezone(String teradataTimezone) {
        this.teradataTimezone = teradataTimezone;
        return this;
    }

    @Config("teradata.export.token")
    @ConfigSecuritySensitive
    public TrinoExportConfig setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
        return this;
    }

    @Config("teradata.export.token-script")
    public TrinoExportConfig setTokenScript(String tokenScript) {
        this.tokenScript = tokenScript;
        return this;
    }

    public String getTokenScript() {
        return tokenScript;
    }

    public String getSecurityToken() {
        if (tokenScript != null && !tokenScript.isEmpty()) {
            return executeScript(tokenScript);
        }
        return securityToken;
    }

    private String executeScript(String scriptPath) {
        try {
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", scriptPath});
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
                String output = reader.readLine();
                if (output != null) {
                    return output.trim();
                }
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                io.airlift.log.Logger.get(TrinoExportConfig.class).error("Script %s exited with code %d", scriptPath, exitCode);
            }
        } catch (Exception e) {
            io.airlift.log.Logger.get(TrinoExportConfig.class).error(e, "Error executing script %s", scriptPath);
        }
        return null;
    }
}
