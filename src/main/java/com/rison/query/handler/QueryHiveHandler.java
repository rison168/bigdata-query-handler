package com.rison.query.handler;

import com.rison.query.pojo.HandlerResult;
import com.rison.query.pojo.QueryExecutionDetail;
import com.rison.query.pojo.QueryExecutionRecord;
import com.rison.query.pojo.ResultType;
import com.rison.query.utils.DatastudioContextHolder;
import com.tencent.tbds.datastudio.pojo.TempQueryExecutionTrace;
import com.tencent.tbds.datastudio.service.kerberos.KerberosServiceImpl;
import com.tencent.tbds.datastudio.service.tempquery.execution.TaskType;
import com.tencent.tbds.datastudio.service.tempquery.websocket.ClientWsBody;
import com.tencent.tbds.datastudio.service.tempquery.websocket.TemporaryQueryWsConnection;
import com.tencent.tbds.datastudio.service.tempquery.websocket.TemporaryQueryWsManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: QueryHiveHandler
 * @USER: Rison
 * @DATE: 2022/10/31 13:58
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class QueryHiveHandler extends QueryAbstractHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryHiveHandler.class);
    @Override
    public Connection getConnection(QueryExecutionRecord queryExecutionRecord, Properties properties) throws Exception {
        Map<String, String> datasource = getHS2Datasource(queryExecutionRecord.getExecutor(), queryExecutionRecord.getDatasourceId(), queryExecutionRecord.getProjectId());
        LOGGER.info("获取的datasource信息：" + datasource);
        String url = datasource.get("url");
        url = url.replace("{database}", queryExecutionRecord.getDatabaseName().trim());
        url = url.replace("default", queryExecutionRecord.getDatabaseName().trim());
        final String username = properties.getProperty("username", "");
        final String clusterId = getClusterId(username, queryExecutionRecord.getDatasourceId());
        final KerberosServiceImpl kerberosService = DatastudioContextHolder.getBean(KerberosServiceImpl.class);
        if (clusterId != null &&
            kerberosService.isKerberos(clusterId)
        ){
            kerberosService.isKerberos(clusterId);
            kerberosService.hiveKerberosLogin(username);
        }
        if (!StringUtils.equalsIgnoreCase(queryExecutionRecord.getDatabaseType(), "hive")){
            throw new IllegalArgumentException(String.format("数据库类型[%s]不是hive", new Object[] { queryExecutionRecord.getDatabaseType() }));
        }
        if (datasource.containsKey("authentication") && datasource.get("authentication").equalsIgnoreCase("LDAP")){
            properties.put("user", properties.get("username"));
        }else if (datasource.containsKey("user_name") && datasource.containsKey("password")){
            properties.put("user", datasource.get("user_name"));
            properties.put("password", datasource.get("password"));
        }else {
            properties.put("user", properties.get("username"));
        }
        properties.remove("username");
        LOGGER.info("***" + properties);
        DriverManager.setLoginTimeout(0);
        return DriverManager.getConnection(url, properties);
    }

    @Override
    public HandlerResult doHandle(QueryExecutionRecord record, QueryExecutionDetail detail, Properties properties, Connection connection) throws Exception {
        if (connection == null){
            connection = getConnection(record, properties);
        }
        final HiveStatement statement = (HiveStatement)connection.createStatement();
        statement.setFetchSize(this.queryProperties.getMaxResultSetSize().intValue());
        String scriptContent = detail.getScriptContent();
        if (scriptContent.endsWith(";")){
            scriptContent = scriptContent.substring(0, scriptContent.length() - 1);
        }
        final boolean isResultSet = statement.execute(scriptContent);
        sendLogs(statement.getQueryLog(), detail.getId());
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
        return handlerResult;
    }

    @PostConstruct
    void init(){
        try{
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }catch (ClassNotFoundException e){
            LOGGER.error("error in TempQueryHiveHandler construction", e);
        }
    }

    private void sendLogs(List<String> queryLog, Long id) {
        final LinkedList<TempQueryExecutionTrace> traces = new LinkedList<>();
        if (queryLog != null && queryLog.size() > 0){
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
            for (String log : queryLog){
                try{
                    LOGGER.info("execution-log is [{}]", log);
                    final String dataStr = log.substring(0, 17).trim();
                    final String level = log.substring(18, 23).trim();
                    traces.add(new TempQueryExecutionTrace(log.substring(26), simpleDateFormat.parse(dataStr), level));
                }catch (Exception e){
                    LOGGER.warn("failed in TempQueryExecutionTrace encapsulation ", e);
                    traces.add(new TempQueryExecutionTrace(log, new Date(), "INFO"));
                }
            }
        }

        if (traces.size() > 0){
            final TemporaryQueryWsConnection connection = TemporaryQueryWsManager.getConnectionBySubtaskId(id);
            if (connection != null){
                final ClientWsBody clientWsBody = new ClientWsBody();
                clientWsBody.setKey(Long.toString(id.longValue()));
                clientWsBody.setType(TaskType.EXECUTION_LOG.getType());
                try{
                    traces.forEach(trace -> {
                        clientWsBody.setData(trace);
                        connection.sendMessage(clientWsBody);
                    });
                }catch (Exception e){
                    LOGGER.warn("failed in TempQueryExecutionTrace transformation ", e);
                }
            }
        }
    }
}
