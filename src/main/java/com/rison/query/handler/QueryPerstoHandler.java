package com.rison.query.handler;

import com.rison.query.pojo.HandlerResult;
import com.rison.query.pojo.QueryExecutionDetail;
import com.rison.query.pojo.QueryExecutionRecord;
import com.rison.query.pojo.ResultType;
import com.tencent.tbds.cluster.client.ClusterClient;
import com.tencent.tbds.cluster.client.conf.ClusterClientConfig;
import com.tencent.tbds.cluster.config.common.ServerType;
import com.tencent.tbds.rpc.meta.ServerInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: QueryPerstoHandler
 * @USER: Rison
 * @DATE: 2022/10/31 14:55
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class QueryPerstoHandler extends QueryAbstractHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(QueryPerstoHandler.class);

    private final Map<String, String> params = new HashMap<>();

    private String catalog;

    @Override
    public Connection getConnection(QueryExecutionRecord record, Properties properties) throws Exception {
        if (record.getDatabaseType().equalsIgnoreCase("iceberg")) {
            Map<String, String> datasource = getDatasource(properties.getProperty("username"), record.getDatasourceId());
            if (datasource.containsKey("catalog")) {
                this.catalog = datasource.get("catalog");
            } else {
                throw new IllegalArgumentException("请先配置数据源自定义参数[catalog]");
            }
        } else {
            this.catalog = StringUtils.lowerCase(record.getDatabaseType());
        }
        final String clusterId = getClusterId(properties.getProperty("username"), record.getDatasourceId());
        final ClusterClientConfig config = new ClusterClientConfig();
        config.setClusterId(clusterId);
        config.setServerType(ServerType.PRESTO.name());
        final ClusterClient client = ClusterClient.getInstance(config);
        final Properties serverProperties = client.getServerProperties();
        String url = null;
        if ("TBDS".equalsIgnoreCase(client.getClusterInfo().getClusterType())) {
            url = "jdbc:persto://";
        } else {
            url = "jdbc:trino://";
        }
        url = url + properties.getProperty("host") + ":" + properties.getProperty("port") + "/" + this.catalog +
                "/" + record.getDatabaseName();

        if (StringUtils.isNotBlank(this.queryProperties.getPresto().getParams())) {
            url = url + "?" + this.queryProperties.getPresto().getParams();
            if (!this.params.containsKey("user")) {
                url = url + "&user=" + record.getExecutor();
            } else {
                url = url + "?user=" + record.getExecutor();
            }
        }
        this.LOGGER.info("presto url:" + url);
        return DriverManager.getConnection(url);
    }

    @Override
    public HandlerResult doHandle(QueryExecutionRecord record, QueryExecutionDetail detail, Properties properties, Connection connection) throws Exception {
        if (connection == null){
            connection = getConnection(record, properties);
        }
        final Statement statement = connection.createStatement();
        statement.setFetchSize(this.queryProperties.getMaxResultSetSize().intValue());
        String scriptContent = detail.getScriptContent();
        if (scriptContent.endsWith(";")){
            scriptContent = scriptContent.substring(0, scriptContent.length() -1 );
        }
        final boolean isResultSet = statement.execute(scriptContent);
        final HandlerResult handlerResult = new HandlerResult();
        if (isResultSet) {
            handlerResult.setResultSet(statement.getResultSet());
            handlerResult.setType(ResultType.RESULT_SET);
        } else if (statement.getUpdateCount() > 0) {
            handlerResult.setUpdateSize(Integer.valueOf(statement.getUpdateCount()));
            handlerResult.setType(ResultType.UPDATE_SIZE);
        } else {
            handlerResult.setType(ResultType.NONE);
        }
        this.LOGGER.info("*****" + isResultSet + "-" + statement.getResultSet() + "-" + statement.getUpdateCount() + "-" + result);
        return handlerResult;
    }

    @PostConstruct
    void init(){
        if (StringUtils.isNotBlank(this.queryProperties.getPresto().getParams())){
            final String[] props = this.queryProperties.getPresto().getParams().split("&");
            for (String prop : props){
                int index = prop.indexOf("=");
                if (index > 0){
                    final String key = prop.substring(0, index);
                    if (StringUtils.isNotBlank(key)){
                        this.params.put(key, prop.substring(index + 1));
                    }else {
                        this.LOGGER.error("presto params [presto.params={}] is not valid ", this.queryProperties.getPresto().getParams());
                        this.params.clear();
                        return;
                    }
                }else {
                    this.LOGGER.error("Presto params[temporary-query.presto.params={}] is not valid", this.queryProperties
                            .getPresto().getParams());
                    this.params.clear();
                    return;
                }
            }
        }
    }
}
