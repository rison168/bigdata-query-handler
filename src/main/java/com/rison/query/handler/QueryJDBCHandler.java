package com.rison.query.handler;


import com.rison.query.pojo.HandlerResult;
import com.rison.query.pojo.QueryExecutionDetail;
import com.rison.query.pojo.QueryExecutionRecord;
import com.rison.query.pojo.ResultType;
import com.rison.query.utils.DatastudioContextHolder;
import com.tencent.tbds.datastudio.service.kerberos.KerberosServiceImpl;
import com.tencent.tbds.datastudio.service.tempquery.handler.JDBCType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: QueryJDBCHandler
 * @USER: Rison
 * @DATE: 2022/10/31 10:36
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class QueryJDBCHandler extends QueryAbstractHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryJDBCHandler.class);


    @Override
    public Connection getConnection(QueryExecutionRecord queryExecutionRecord, Properties properties) throws Exception {
        Map<String, String> datasource;
        String url = null, key = queryExecutionRecord.getDatasourceId() + "|" + queryExecutionRecord.getDatabaseName();
        String username = properties.getProperty("username", "");
        final String clusterId = getClusterId(username, queryExecutionRecord.getDatasourceId());
        final KerberosServiceImpl kerberosService = DatastudioContextHolder.getBean(KerberosServiceImpl.class);
        if (clusterId != null && queryExecutionRecord.getDatabaseType().equalsIgnoreCase("hive") &&
                kerberosService.isKerberos(clusterId)
        ) {
            kerberosService.hiveKerberosLogin(username);
        }
        if ("HIVE".equalsIgnoreCase(queryExecutionRecord.getDatabaseType())) {
            datasource = getHS2Datasource(queryExecutionRecord.getExecutor(), queryExecutionRecord.getDatasourceId(), queryExecutionRecord.getProjectId());
        } else {
            datasource = getDatasource(queryExecutionRecord.getExecutor(), queryExecutionRecord.getDatasourceId());
        }
        LOGGER.info("获取的datasource信息：" + datasource);
        if (datasource == null || datasource.isEmpty()) {
            throw new IllegalAccessException("请先配置数据源连接参数[url、host、port 、user_name 、password]");
        }
        final JDBCType jdbcType = JDBCType.getFromName(queryExecutionRecord.getDatabaseType());
        if (jdbcType == null) {
            if (datasource.containsKey("jdbc-driver-class")) {
                Class.forName(datasource.get("jdbc-driver-class"));
            } else {
                throw new IllegalAccessException(String.format("不支持的dbType[%s]", new Object[]{queryExecutionRecord.getDatabaseType()}));
            }
        }
        if (datasource.containsKey("authentication") && datasource.get("authentication").equalsIgnoreCase("LDAP")) {
            properties.put("user", properties.get("username"));
        } else if (datasource.containsKey("user_name") && datasource.containsKey("password")){
            properties.put("user", datasource.get("user_name"));
            properties.put("password", datasource.get("password"));
        }else {
            properties.put("user", properties.get("username"));
        }
        properties.remove("username");
        if (datasource.containsKey("url")){
            url = datasource.get("url");
            if (url != null){
                url = jdbcType.generateUrl(url, queryExecutionRecord);
            }else {
                final String host = datasource.get("host");
                final String port = datasource.get("port");
                final String[] hosts = host.split(",");
                final StringBuilder urlBuffer = new StringBuilder();
                final StringBuilder append = urlBuffer.append(jdbcType.getProtocol());
                jdbcType.generateProtocolSuffix(urlBuffer);
                boolean portIsNotBlank = StringUtils.isNotBlank(port);
                if (host.length() > 0){
                    urlBuffer.append(hosts[0]);
                    if (portIsNotBlank){
                        urlBuffer.append(":");
                        urlBuffer.append(port);
                    }
                    for (int i = 1; i < hosts.length; i++ ){
                        urlBuffer.append(",");
                        urlBuffer.append(hosts[i]);
                        if (portIsNotBlank){
                            urlBuffer.append(":");
                            urlBuffer.append(port);
                        }
                    }
                }
                jdbcType.generateUrlSuffix(datasource, urlBuffer, queryExecutionRecord.getDatabaseName());
                url = urlBuffer.toString();
            }
        }
        if ("hive".equalsIgnoreCase(queryExecutionRecord.getDatabaseType())){
            DriverManager.setLoginTimeout(0);
        }
        if ("ES".equalsIgnoreCase(queryExecutionRecord.getDatabaseType())){
            return DriverManager.getConnection(url, datasource.get("user_name"), datasource.get("password"));
        }
        final Connection connection = DriverManager.getConnection(url, properties);
        return connection;
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
            scriptContent = scriptContent.substring(0, scriptContent.length() - 1);
        }
        final boolean isResultSet = statement.execute(scriptContent);
        final HandlerResult result = new HandlerResult();
        if (isResultSet){
            result.setResultSet(statement.getResultSet());
            result.setType(ResultType.RESULT_SET);
        } else if (statement.getUpdateCount() > 0){
            result.setUpdateSize(Integer.valueOf(statement.getUpdateCount()));
            result.setType(ResultType.RESULT_SET);
        }else {
            result.setType(ResultType.NONE);
        }
        return result;
    }

    @PostConstruct
    void init(){
        for (JDBCType type : JDBCType.values()){
            try{
                Class.forName(type.getDriver());
            }catch (ClassNotFoundException e){
                LOGGER.error("error in JDBC Driver construction", e);
            }
        }
    }
    
}

/**
 * url - jdbc:hive2://tbds-10-1-0-97:2181,tbds-10-1-0-63:2181,tbds-10-1-0-87:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_default
 * hive_version - 3.1.2-TBDS-5.2.0.1
 * mysql_user_password - ******
 * port - 10001
 * server_ha - true
 * zk_namespace - hiveserver2_default
 * driverClassName - org.apache.hive.jdbc.HiveDriver
 * thrift_metastore - thrift://tbds-10-1-0-97:9083,thrift://tbds-10-1-0-87:9083
 * mysql_url - jdbc:mysql://tdw.mysql.tbds.com:3306/hive?characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true
 * mode - http
 * mysql_user_name - hive
 * cluster_type - TBDS Lite
 * authentication - LDAP
 * hive_servers - tbds-10-1-0-104,tbds-10-1-0-84
 * shell_path - /usr/local/cluster-shim/lite/500/bin
 * host - tbds-10-1-0-97:2181,tbds-10-1-0-63:2181,tbds-10-1-0-87:2181
 **/