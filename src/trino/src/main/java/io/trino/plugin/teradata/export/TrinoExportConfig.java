package io.trino.plugin.teradata.export;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import java.util.concurrent.TimeUnit;

public class TrinoExportConfig {
    // === Core Connection Settings ===
    private String teradataUrl;
    private String teradataUser;
    private String teradataPassword;
    private String passwordScript;
    private String teradataTimezone = "-05:00";

    // === Network/Port Settings ===
    private int flightPort = 50051;
    private int bridgePort = 9999;
    private String trinoAddress = "172.27.251.157";
    private String flightBindAddress = "0.0.0.0";
    private String workerAdvertisedAddresses;  // For NAT/multi-homed networks

    // === Buffer/Performance Settings ===
    private int batchSize = 100000;
    private int socketReceiveBufferSize = 64 * 1024 * 1024;  // 64MB
    private int inputBufferSize = 8 * 1024 * 1024;           // 8MB
    private int bufferQueueCapacity = 100;
    private long pagePollTimeoutMs = 500;
    private int splitsPerWorker = 4;

    // === Dynamic Filtering Settings ===
    private Duration dynamicFilterTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean enableDynamicFiltering = true;
    private boolean enableAggregationPushdown = true;
    private boolean enableTopNPushdown = true;

    // === Security Settings ===
    private String securityToken;
    private String tokenScript;
    private boolean enforceProxyAuthentication = true;

    // === UDF Settings ===
    private String udfDatabase = "TrinoExport";
    private String udfName = "ExportToTrino";

    // === Schema Settings ===
    private String defaultSchemas = "TrinoExport,default";

    // === Logging Settings ===
    private boolean enableDebugLogging = false;

    // ============================================================
    // Core Connection Getters/Setters
    // ============================================================

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

    public String getTeradataTimezone() {
        return teradataTimezone;
    }

    @Config("teradata.timezone")
    public TrinoExportConfig setTeradataTimezone(String teradataTimezone) {
        this.teradataTimezone = teradataTimezone;
        return this;
    }

    // ============================================================
    // Network/Port Getters/Setters
    // ============================================================

    public int getFlightPort() {
        return flightPort;
    }

    @Config("teradata.export.flight-port")
    public TrinoExportConfig setFlightPort(int flightPort) {
        this.flightPort = flightPort;
        return this;
    }

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

    public String getFlightBindAddress() {
        return flightBindAddress;
    }

    @Config("teradata.export.flight-bind-address")
    public TrinoExportConfig setFlightBindAddress(String flightBindAddress) {
        this.flightBindAddress = flightBindAddress;
        return this;
    }

    public String getWorkerAdvertisedAddresses() {
        return workerAdvertisedAddresses;
    }

    @Config("teradata.export.worker-advertised-addresses")
    public TrinoExportConfig setWorkerAdvertisedAddresses(String workerAdvertisedAddresses) {
        this.workerAdvertisedAddresses = workerAdvertisedAddresses;
        return this;
    }

    // ============================================================
    // Buffer/Performance Getters/Setters
    // ============================================================

    public int getBatchSize() {
        return batchSize;
    }
    
    @Config("teradata.export.batch-size")
    public TrinoExportConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    @Config("teradata.export.socket-receive-buffer-size")
    public TrinoExportConfig setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
        return this;
    }

    public int getInputBufferSize() {
        return inputBufferSize;
    }

    @Config("teradata.export.input-buffer-size")
    public TrinoExportConfig setInputBufferSize(int inputBufferSize) {
        this.inputBufferSize = inputBufferSize;
        return this;
    }

    public int getBufferQueueCapacity() {
        return bufferQueueCapacity;
    }

    @Config("teradata.export.buffer-queue-capacity")
    public TrinoExportConfig setBufferQueueCapacity(int bufferQueueCapacity) {
        this.bufferQueueCapacity = bufferQueueCapacity;
        return this;
    }

    public long getPagePollTimeoutMs() {
        return pagePollTimeoutMs;
    }

    @Config("teradata.export.page-poll-timeout-ms")
    public TrinoExportConfig setPagePollTimeoutMs(long pagePollTimeoutMs) {
        this.pagePollTimeoutMs = pagePollTimeoutMs;
        return this;
    }

    public int getSplitsPerWorker() {
        return splitsPerWorker;
    }

    @Config("teradata.export.splits-per-worker")
    public TrinoExportConfig setSplitsPerWorker(int splitsPerWorker) {
        this.splitsPerWorker = splitsPerWorker;
        return this;
    }

    // ============================================================
    // Dynamic Filtering Getters/Setters
    // ============================================================

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

    public boolean isEnableAggregationPushdown() {
        return enableAggregationPushdown;
    }

    @Config("teradata.export.enable-aggregation-pushdown")
    public TrinoExportConfig setEnableAggregationPushdown(boolean enableAggregationPushdown) {
        this.enableAggregationPushdown = enableAggregationPushdown;
        return this;
    }

    public boolean isEnableTopNPushdown() {
        return enableTopNPushdown;
    }

    @Config("teradata.export.enable-topn-pushdown")
    public TrinoExportConfig setEnableTopNPushdown(boolean enableTopNPushdown) {
        this.enableTopNPushdown = enableTopNPushdown;
        return this;
    }

    // ============================================================
    // Security Getters/Setters
    // ============================================================

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

    public boolean isEnforceProxyAuthentication() {
        return enforceProxyAuthentication;
    }

    @Config("teradata.export.enforce-proxy-authentication")
    public TrinoExportConfig setEnforceProxyAuthentication(boolean enforceProxyAuthentication) {
        this.enforceProxyAuthentication = enforceProxyAuthentication;
        return this;
    }

    // ============================================================
    // UDF Settings Getters/Setters
    // ============================================================

    public String getUdfDatabase() {
        return udfDatabase;
    }

    @Config("teradata.export.udf-database")
    public TrinoExportConfig setUdfDatabase(String udfDatabase) {
        this.udfDatabase = udfDatabase;
        return this;
    }

    public String getUdfName() {
        return udfName;
    }

    @Config("teradata.export.udf-name")
    public TrinoExportConfig setUdfName(String udfName) {
        this.udfName = udfName;
        return this;
    }

    // ============================================================
    // Schema Settings Getters/Setters
    // ============================================================

    public String getDefaultSchemas() {
        return defaultSchemas;
    }

    @Config("teradata.export.default-schemas")
    public TrinoExportConfig setDefaultSchemas(String defaultSchemas) {
        this.defaultSchemas = defaultSchemas;
        return this;
    }

    public String[] getDefaultSchemasArray() {
        if (defaultSchemas == null || defaultSchemas.isEmpty()) {
            return new String[0];
        }
        return defaultSchemas.split(",");
    }

    // ============================================================
    // Logging Settings Getters/Setters
    // ============================================================

    public boolean isEnableDebugLogging() {
        return enableDebugLogging;
    }

    @Config("teradata.export.enable-debug-logging")
    public TrinoExportConfig setEnableDebugLogging(boolean enableDebugLogging) {
        this.enableDebugLogging = enableDebugLogging;
        return this;
    }

    // ============================================================
    // Utility Methods
    // ============================================================

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
