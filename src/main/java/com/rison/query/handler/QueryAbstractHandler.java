package com.rison.query.handler;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLParserFeature;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.rison.query.pojo.HandlerResult;
import com.rison.query.pojo.QueryExecutionDetail;
import com.rison.query.pojo.QueryExecutionRecord;
import com.rison.query.pojo.ResultType;
import com.rison.query.storage.ResultStorageType;
import com.tencent.tbds.datastudio.common.TempQueryProperties;
import com.tencent.tbds.datastudio.service.UtherRpcService;
import com.tencent.tbds.datastudio.service.tempquery.output.CsvFileOutput;
import com.tencent.tbds.metadata.api.vo.ServerInfoVO;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.jasig.cas.client.authentication.AttributePrincipalImpl;
import org.jasig.cas.client.util.AssertionHolder;
import org.jasig.cas.client.validation.Assertion;
import org.jasig.cas.client.validation.AssertionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: QueryAbstractHandler
 * @USER: Rison
 * @DATE: 2022/10/28 14:47
 * @PROJECT_NAME: bigdata-query-handler
 **/
public abstract class QueryAbstractHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(QueryAbstractHandler.class);
    protected CsvFileOutput csvFileOutput;

    protected TempQueryProperties queryProperties;

    protected UtherRpcService utherRpcService;

    public abstract Connection getConnection(QueryExecutionRecord queryExecutionRecord, Properties properties) throws Exception;

    public abstract HandlerResult doHandle(QueryExecutionRecord record, QueryExecutionDetail detail, Properties properties, Connection connection) throws Exception;

    public HandlerResult handle(QueryExecutionRecord record, QueryExecutionDetail detail, Properties properties, Connection connection) throws Exception {
        if (detail.getScriptContent() != null) {
            SQLStatement sqlStatement = null;
            try {
                DbType dbType = DbType.of(record.getEngineName());
                if (dbType == null) {
                    dbType = DbType.of(StringUtils.lowerCase(record.getDatabaseType()));
                    sqlStatement = SQLUtils.parseSingleStatement(detail.getScriptContent(), dbType, new SQLParserFeature[0]);
                }
            } catch (Exception exception) {
            }
            if (sqlStatement instanceof com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement) {
                throw new RuntimeException("不允许建库!" + detail.getScriptContent());
            }
            if (sqlStatement instanceof com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement) {
                throw new RuntimeException("不允许删库!" + detail.getScriptContent());
            }
            if (sqlStatement instanceof com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement) {
                throw new RuntimeException("不允许修改库属性" + detail.getScriptContent());
            }
        }
        return doHandle(record, detail, properties, connection);
    }


    public JSON parseResultSetToJson(ResultSet rs) throws SQLException {
        final JSONArray json = new JSONArray();
        if (rs == null) {
            return (JSON) json;
        }
        ResultSetMetaData metaData = rs.getMetaData();
//    int returnSize = 0;
        while (rs.next()) {
            final int columnCount = metaData.getColumnCount();
            final JSONObject obj = new JSONObject(true);
            for (int i = 1; i < columnCount + 1; i++) {
                String column_name = metaData.getColumnName(i);
                switch (metaData.getColumnType(i)) {
                    case 2003:
                        obj.put(column_name, rs.getArray(column_name));
                        break;
                    case -5:
                        obj.put(column_name, Long.valueOf(rs.getLong(column_name)));
                        break;
                    case -6:
                    case 4:
                    case 5:
                        obj.put(column_name, Integer.valueOf(rs.getInt(column_name)));
                        break;
                    case 16:
                        obj.put(column_name, Boolean.valueOf(rs.getBoolean(column_name)));
                        break;
                    case 2004:
                        obj.put(column_name, rs.getBlob(column_name));
                        break;
                    case 8:
                        obj.put(column_name, Double.valueOf(rs.getDouble(column_name)));
                        break;
                    case 6:
                        obj.put(column_name, Float.valueOf(rs.getFloat(column_name)));
                        break;
                    case -9:
                        obj.put(column_name, rs.getNString(column_name));
                        break;
                    case 12:
                        obj.put(column_name, rs.getString(column_name));
                        break;
                    case 91:
                        obj.put(column_name, rs.getDate(column_name));
                        break;
                    case 93:
                        obj.put(column_name, rs.getTimestamp(column_name));
                        break;
                    case -3:
                        obj.put(column_name, new String(rs.getBytes(column_name)));
                        break;
                    default:
                        obj.put(column_name, rs.getObject(column_name));
                        break;
                }
            }
            json.add(obj);
        }
        return json;
    }

    private String getResultAsString(ResultType resultType, Object resultObj) {
        JSONObject jsonObject;
        if (resultObj == null) {
            return null;
        }
        switch (resultType) {
            case RESULT_SET:
            case SPARK_DATASET:
                return ((JSON) resultObj).toString();
            case UPDATE_SIZE:
                jsonObject = new JSONObject();
                jsonObject.put("update rows count", resultObj);
                return jsonObject.toJSONString();
        }
        return null;
    }

    public String saveResultToFs(QueryExecutionDetail detail, String fsType, ResultType resultType, Object resultObj){
        final String filePath = ResultStorageType.getStorageBean(fsType).upload(getResultAsString(resultType, resultObj));
        this.LOGGER.info("save result");
        return filePath;
    }

    public Object parseHandlerResult(HandlerResult result) throws SQLException {
        ResultSet resultSet;
        JSONObject jsonObject;
        JSONArray jsonArray;
        if (result == null) {
            return null;
        }
        switch (result.getType()) {
            case RESULT_SET:
                resultSet = (ResultSet)result.getResultSet();
                try {
                    return parseResultSetToJson(resultSet);
                } finally {
                    try {
                        resultSet.close();
                    } catch (Exception e) {
                        this.LOGGER.warn("cannot close ResultSet", e);
                    }
                }
            case UPDATE_SIZE:
                jsonObject = new JSONObject();
                jsonObject.put("update rows count", result.getUpdateSize());
                jsonArray = new JSONArray();
                jsonArray.add(jsonObject);
                return jsonArray;
            case SPARK_DATASET:
                return result.getResultSet();
        }
        return null;
    }
    protected Map<String, String> getDatasource(String username, String datasourceId) throws Exception {
        AttributePrincipalImpl userPrincipal = new AttributePrincipalImpl(username, Maps.<String, Object>newHashMap());
        AssertionImpl userAssertion = new AssertionImpl((AttributePrincipal)userPrincipal);
        AssertionHolder.setAssertion((Assertion)userAssertion);
        ServerInfoVO datasource = this.utherRpcService.getServerInfo(datasourceId);
        Map<String, String> properties = (Map<String, String>)JSON.parseObject(datasource.getProperties(), HashMap.class);
        Map<String, String> configs = (Map<String, String>)JSON.parseObject(datasource.getConfigs(), HashMap.class);
        if (configs != null && !configs.isEmpty()) {
            properties.putAll(configs);
        }
        return properties;
    }

    protected Map<String, String> getHS2Datasource(String username, String datasourceId, String projectId) throws Exception {
        AttributePrincipalImpl userPrincipal = new AttributePrincipalImpl(username, Maps.<String, Object>newHashMap());
        AssertionImpl userAssertion = new AssertionImpl((AttributePrincipal)userPrincipal);
        AssertionHolder.setAssertion((Assertion)userAssertion);
        ServerInfoVO datasource = this.utherRpcService.getServerInfoForHS2Isolation(datasourceId, projectId);
        Map<String, String> properties = (Map<String, String>)JSON.parseObject(datasource.getProperties(), HashMap.class);
        Map<String, String> configs = (Map<String, String>)JSON.parseObject(datasource.getConfigs(), HashMap.class);
        if (configs != null && !configs.isEmpty()) {
            properties.putAll(configs);
        }
        return properties;
    }

    protected String getClusterId(String username, String datasourceId) throws Exception {
        try {
            AttributePrincipalImpl userPrincipal = new AttributePrincipalImpl(username, Maps.<String, Object>newHashMap());
            AssertionImpl userAssertion = new AssertionImpl((AttributePrincipal)userPrincipal);
            AssertionHolder.setAssertion((Assertion)userAssertion);
            ServerInfoVO datasource = this.utherRpcService.getServerInfo(datasourceId);
            return datasource.getClusterId();
        } catch (Exception e) {
            throw new Exception("failed to get clusterId,error: " + e);
        }
    }
}
/**
 * {
 * "database": "tx_hivedb",
 * "databaseType": "hive",
 * "sourceId": "f53a8fe2-479d-4e78-bcd3-153f9aee7edf",
 * "engine": "spark",
 * "scriptContent": "INSERT OVERWRITE hdwd.dwd_user
 * SELECT id, name, context, type_id, desc, ts
 * FROM (
 * SELECT id, name,context,u.type_id,desc,ts
 * FROM hods.ods_user u
 * LEFT JOIN hods.ods_type t ON t.type_id = u.type_id
 * );
 * ",
 * "projectId": 1
 * }
 */
