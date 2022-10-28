package com.rison.query.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @PACKAGE_NAME: com.rison.query.common
 * @NAME: QueryProperties
 * @USER: Rison
 * @DATE: 2022/10/29 0:48
 * @PROJECT_NAME: bigdata-query-handler
 **/
@Component
@ConfigurationProperties(prefix = "temporary-query")
public class QueryProperties {
    private final ExecutorProperty tempQueryPool = new ExecutorProperty();

    private final ExecutorProperty sqlCheckPool = new ExecutorProperty();

    private final Engine presto = new Engine();

    private final Engine hive = new Engine();

    private final Engine spark = new Engine();

    private final ExternalApi guldan = new ExternalApi();

    private Integer recordScriptContentMaxSize = Integer.valueOf(1000);

    private Integer detailScriptContentMaxSize = Integer.valueOf(1000);

    private List<String> supportedExecutionEngines = Arrays.asList(new String[] { "presto", "hive", "jdbc" });

    private String localTmpDir = "/tmp/";

    private String resultStorageType = "LOCAL";

    private String hdfsRootDir = "/datastudio/temporary-query/";

    private Integer maxResultSetSize = Integer.valueOf(1000);

    private Integer sparkTaskWaitTimeNumber = Integer.valueOf(25);

    private Long taskWaitTimeout = Long.valueOf(60000L);

    private Long taskSleepInterval = Long.valueOf(2000L);

    private List<String> skipScriptTokens = Collections.emptyList();

    public ExecutorProperty getTempQueryPool() {
        return this.tempQueryPool;
    }

    public ExecutorProperty getSqlCheckPool() {
        return this.sqlCheckPool;
    }

    public Engine getPresto() {
        return this.presto;
    }

    public Engine getHive() {
        return this.hive;
    }

    public Engine getSpark() {
        return this.spark;
    }

    public ExternalApi getGuldan() {
        return this.guldan;
    }

    public Integer getRecordScriptContentMaxSize() {
        return this.recordScriptContentMaxSize;
    }

    public void setRecordScriptContentMaxSize(Integer recordScriptContentMaxSize) {
        this.recordScriptContentMaxSize = recordScriptContentMaxSize;
    }

    public Integer getDetailScriptContentMaxSize() {
        return this.detailScriptContentMaxSize;
    }

    public void setDetailScriptContentMaxSize(Integer detailScriptContentMaxSize) {
        this.detailScriptContentMaxSize = detailScriptContentMaxSize;
    }

    public List<String> getSupportedExecutionEngines() {
        return this.supportedExecutionEngines;
    }

    public void setSupportedExecutionEngines(List<String> supportedExecutionEngines) {
        this.supportedExecutionEngines = supportedExecutionEngines;
    }

    public String getLocalTmpDir() {
        return this.localTmpDir;
    }

    public void setLocalTmpDir(String localTmpDir) {
        this.localTmpDir = localTmpDir;
    }

    public String getResultStorageType() {
        return this.resultStorageType;
    }

    public void setResultStorageType(String resultStorageType) {
        this.resultStorageType = resultStorageType;
    }

    public String getHdfsRootDir() {
        return this.hdfsRootDir;
    }

    public void setHdfsRootDir(String hdfsRootDir) {
        this.hdfsRootDir = hdfsRootDir;
    }

    public Integer getMaxResultSetSize() {
        return this.maxResultSetSize;
    }

    public void setMaxResultSetSize(Integer maxResultSetSize) {
        this.maxResultSetSize = maxResultSetSize;
    }

    public Long getTaskWaitTimeout() {
        return this.taskWaitTimeout;
    }

    public void setTaskWaitTimeout(Long taskWaitTimeout) {
        this.taskWaitTimeout = taskWaitTimeout;
    }

    public Long getTaskSleepInterval() {
        return this.taskSleepInterval;
    }

    public void setTaskSleepInterval(Long taskSleepInterval) {
        this.taskSleepInterval = taskSleepInterval;
    }

    public Integer getSparkTaskWaitTimeNumber() {
        return this.sparkTaskWaitTimeNumber;
    }

    public void setSparkTaskWaitTimeNumber(Integer sparkTaskWaitTimeNumber) {
        this.sparkTaskWaitTimeNumber = sparkTaskWaitTimeNumber;
    }

    public List<String> getSkipScriptTokens() {
        return this.skipScriptTokens;
    }

    public void setSkipScriptTokens(List<String> skipScriptTokens) {
        this.skipScriptTokens = skipScriptTokens;
    }

    public static class ExecutorProperty {
        private Integer coreSize = Integer.valueOf(2);

        private Integer maximumSize = Integer.valueOf(10);

        private Long keepAliveMillisecond = Long.valueOf(200L);

        private Integer blockingQueueCapacity = Integer.valueOf(1000);

        public Integer getCoreSize() {
            return this.coreSize;
        }

        public void setCoreSize(Integer coreSize) {
            this.coreSize = coreSize;
        }

        public Integer getMaximumSize() {
            return this.maximumSize;
        }

        public void setMaximumSize(Integer maximumSize) {
            this.maximumSize = maximumSize;
        }

        public Long getKeepAliveMillisecond() {
            return this.keepAliveMillisecond;
        }

        public void setKeepAliveMillisecond(Long keepAliveMillisecond) {
            this.keepAliveMillisecond = keepAliveMillisecond;
        }

        public Integer getBlockingQueueCapacity() {
            return this.blockingQueueCapacity;
        }

        public void setBlockingQueueCapacity(Integer blockingQueueCapacity) {
            this.blockingQueueCapacity = blockingQueueCapacity;
        }
    }

    public static class Engine {
        private String protocol = "http";

        private String hosts = "";

        private String params = "";

        private String path = "";

        public String getProtocol() {
            return this.protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getHosts() {
            return this.hosts;
        }

        public void setHosts(String hosts) {
            this.hosts = hosts;
        }

        public String getParams() {
            return this.params;
        }

        public void setParams(String params) {
            this.params = params;
        }

        public String getPath() {
            return this.path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    public static class ExternalApi {
        private String host = "";

        private String scriptRenderApi = "/tdw/guldan/api/taskScheduler/param/selectProjectSqlParam";

        private String scriptRenderWorkAndTaskParamsApi = "/tdw/guldan/tbds/inner/service/param/selectWorkflowTaskSqlParam";

        public String getHost() {
            return this.host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getScriptRenderApi() {
            return this.scriptRenderApi;
        }

        public void setScriptRenderApi(String scriptRenderApi) {
            this.scriptRenderApi = scriptRenderApi;
        }

        public void setScriptRenderWorkAndTaskParamsApi(String scriptRenderWorkAndTaskParamsApi) {
            this.scriptRenderWorkAndTaskParamsApi = scriptRenderWorkAndTaskParamsApi;
        }

        public String getScriptRenderWorkAndTaskParamsApi() {
            return this.scriptRenderWorkAndTaskParamsApi;
        }
    }
}
