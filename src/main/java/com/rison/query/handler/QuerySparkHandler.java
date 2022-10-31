package com.rison.query.handler;

import com.rison.query.handler.QueryAbstractHandler;
import com.rison.query.pojo.HandlerResult;
import com.rison.query.pojo.QueryExecutionDetail;
import com.rison.query.pojo.QueryExecutionRecord;
import com.rison.query.pojo.ResultType;
import com.tencent.tbds.cluster.client.ClusterClient;
import com.tencent.tbds.cluster.client.conf.ClusterClientConfig;
import com.tencent.tbds.cluster.feign.api.ClusterFeignClient;
import com.tencent.tbds.cluster.feign.api.common.CallResult;
import com.tencent.tbds.cluster.feign.api.vo.FairQueueClusterVO;
import com.tencent.tbds.datastudio.exception.InvokeRemoteServerException;
import com.tencent.tbds.datastudio.feign.TbdsPortalClient;
import com.tencent.tbds.datastudio.pojo.LoginUserInfo;
import com.tencent.tbds.datastudio.security.TbdsLoginContext;
import com.tencent.tbds.datastudio.service.tempquery.handler.IcebergCatalogType;
import com.tencent.tbds.datastudio.service.tempquery.handler.JDBCType;
import com.tencent.tbds.datastudio.utils.AccessUtil;
import com.tencent.tbds.datastudio.utils.AuthorizationUtils;
import com.tencent.tbds.kdcproxy.api.KdcProxyFeignApi;
import com.tencent.tbds.kdcproxy.api.util.KerberosUtil;
import com.tencent.tbds.rpc.meta.ClusterInfo;
import com.tencent.tbds.rpc.meta.ShimInfo;
import com.tencent.tbds.tbdsportal.feign.api.vo.AccessKeyVO;
import com.tencent.tbds.tbdsportal.feign.api.vo.UserLoginInfoVO;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.query.pojo
 * @NAME: QuerySparkHandler
 * @USER: Rison
 * @DATE: 2022/10/30 23:44
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class QuerySparkHandler extends QueryAbstractHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuerySparkHandler.class);
    private String filePath;

    private Configuration config;

    private TbdsPortalClient tbdsPortalClient;

    private AuthorizationUtils authorizationUtils;

    private ClusterFeignClient clusterFeignClient;

    private TbdsLoginContext tbdsLoginContext;

    private KdcProxyFeignApi kdcProxyFeignApi;

    private String keytab = "";

    private String principal = "";

    @Autowired
    public void setKdcProxyFeignApi(KdcProxyFeignApi kdcProxyFeignApi) {
        this.kdcProxyFeignApi = kdcProxyFeignApi;
    }

    @Autowired
    public void setTbdsLoginContext(TbdsLoginContext tbdsLoginContext) {
        this.tbdsLoginContext = tbdsLoginContext;
    }

    @Autowired
    public void setAuthorizationUtils(AuthorizationUtils authorizationUtils) {
        this.authorizationUtils = authorizationUtils;
    }

    @Autowired
    public void setPortalService(TbdsPortalClient tbdsPortalClient) {
        this.tbdsPortalClient = tbdsPortalClient;
    }

    @Autowired
    public void setClusterFeignClient(ClusterFeignClient clusterFeignClient) {
        this.clusterFeignClient = clusterFeignClient;
    }

    @Override
    public Connection getConnection(QueryExecutionRecord queryExecutionRecord, Properties properties) throws SQLException {
        if (!JDBCType.HIVE.getName().equalsIgnoreCase(queryExecutionRecord.getDatabaseType())) {
            throw new RuntimeException("Spark 仅仅支持Hive源");
        }
        properties.put("user", properties.getProperty("username", ""));
        LOGGER.debug("properties is :" + properties.toString());
        if ("jdbc:hive2".equals(this.queryProperties.getSpark().getProtocol())) {
            String url = "jdbc:hive2://" + this.queryProperties.getSpark().getHosts() + "/" + queryExecutionRecord.getDatabaseName();
            if (StringUtils.isNotBlank(this.queryProperties.getSpark().getParams())) {
                url = url + "?" + this.queryProperties.getSpark().getParams();
            }
            LOGGER.debug("kyuubi url is {}", url);
            return DriverManager.getConnection(url, properties);
        }
        return null;
    }

    private Configuration getConf(Properties connProps, String authentication, String confDir) throws Exception {
        this.config = new Configuration();
        final String username = connProps.getProperty("username", "");
        if ("tbds".equalsIgnoreCase(authentication)) {
            this.config.set("hadoop_security_authentication_tbds_username", username);
            this.config.set("hadoop_security_authentication_tbds_secureid", connProps
                    .getProperty("hadoop_security_authentication_tbds_secureid"));
            this.config.set("hadoop_security_authentication_tbds_securekey", connProps
                    .getProperty("hadoop_security_authentication_tbds_securekey"));
        }
        this.config.set("hadoop.security.authentication", authentication);
        this.config.addResource(new Path(confDir + "/hdfs/hdfs-site.xml"));
        this.config.addResource(new Path(confDir + "/hdfs/core-site.xml"));
        UserGroupInformation.setConfiguration(this.config);
        if ("kerberos".equalsIgnoreCase(authentication)) {
            this.keytab = KerberosUtil.getKeytabRemote(username, this.kdcProxyFeignApi);
            this.principal = KerberosUtil.getPrincipalByUsername(username);
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            UserGroupInformation.loginUserFromKeytab(this.principal, this.keytab);
        } else {
            UserGroupInformation.loginUserFromSubject(null);
        }
        return this.config;
    }

    public String getYarnQueueName(QueryExecutionRecord record, String userName, String clusterId) throws Exception {
        final LoginUserInfo loginUserInfo = this.tbdsLoginContext.getUserInfo(userName);
        final UserLoginInfoVO userLoginInfoVO = this.tbdsPortalClient.whoami(this.authorizationUtils.getPortalUserAuthorization(loginUserInfo.getUserId()));
        final String projectId = record.getProjectId();
        final String queueId = this.tbdsPortalClient.getQueueId(Integer.parseInt(projectId), clusterId, userLoginInfoVO);
        LOGGER.info("***" + queueId + "-" + projectId + "-" + clusterId);
        if (queueId == null) {
            return "root.default";
        }
        final AccessKeyVO userAccessKeyDocument = this.authorizationUtils.getUserAccessKeyDocument(Integer.parseInt(userLoginInfoVO.getUserId()));
        final String accessAuthHeader = AccessUtil.getAccessAuthHeader(userAccessKeyDocument.getSecureId(), userAccessKeyDocument.getSecureKey());
        final CallResult<FairQueueClusterVO> fairQueueClusterVOCallResult = this.clusterFeignClient.queueInfo(accessAuthHeader, queueId);
        final FairQueueClusterVO resultData = fairQueueClusterVOCallResult.getResultData();
        return resultData.getFullName();
    }

    @Override
    public HandlerResult doHandle(QueryExecutionRecord record, QueryExecutionDetail detail, Properties properties, Connection connection) throws Exception {
        if (!JDBCType.HIVE.getName().equalsIgnoreCase(record.getDatabaseType()) && !JDBCType.ICEBERG.getName().equalsIgnoreCase(record.getDatabaseType())) {
            throw new RuntimeException("spark 只支持 hive 源 和 iceberg 源");
        }
        String scriptContent = detail.getScriptContent();
        if (scriptContent != null) {
            scriptContent = scriptContent.trim();
            if (scriptContent.length() > 0 && scriptContent.endsWith(";")) {
                scriptContent = scriptContent.substring(0, scriptContent.length() - 1);
            }
        }
        if ("jdbc.hive2".equals(this.queryProperties.getSpark().getProtocol())) {
            if (connection == null) {
                connection = getConnection(record, properties);
            }
            final Statement statement = connection.createStatement();
            statement.setFetchSize(this.queryProperties.getMaxResultSetSize().intValue());
            final boolean isExecute = statement.execute(scriptContent);
            final HandlerResult handlerResult = new HandlerResult();
            if (isExecute) {
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
        String user = properties.getProperty("username", "");
        String secureid = properties.getProperty("hadoop_security_authentication_tbds_secureid");
        String securekey = properties.getProperty("hadoop_security_authentication_tbds_securekey");
        String clusterId = getClusterId(user, record.getDatasourceId());
        String sparkQueueName = getYarnQueueName(record, user, clusterId);
        final ClusterClientConfig clusterClientConfig = new ClusterClientConfig();
        clusterClientConfig.setClusterId(clusterId);
        final ClusterClient client = ClusterClient.getInstance(clusterClientConfig);
        final ClusterInfo clusterInfo = client.getClusterInfo();
        final ShimInfo clusterShimInfo = client.getClusterShimInfo();
        final String authenticationType = clusterInfo.getAuthenticationType();
        this.config = getConf(properties, authenticationType, clusterShimInfo.getConfDir());
        final HashMap<String, String> hashMap = new HashMap<>();
        if ("tbds".equalsIgnoreCase(authenticationType)) {
            hashMap.put("hadoop_security_authentication_tbds_secureid", secureid);
            hashMap.put("hadoop_security_authentication_tbds_securekey", securekey);
            hashMap.put("hadoop_security_authentication_tbds_username", user);
        } else {
            hashMap.put("HADOOP_USER_NAME", user);
        }
        hashMap.put("SPARK_HOME", clusterShimInfo.getBaseDir() + "/lib/spark");
        hashMap.put("HADOOP_CONF_DIR", clusterShimInfo.getConfDir() + "/hdfs");
        hashMap.put("SPARK_CONF_DIR", clusterShimInfo.getConfDir() + "/spark");
        String detailId = detail.getId().toString();
        String mainClass = "com.rison.query.job.SparkSQLJob";
        final SparkLauncher spark = new SparkLauncher(hashMap);
        final SparkLauncher sparkLauncher = spark.setAppName("app")
                .setAppResource(this.filePath)
                .setMainClass(mainClass)
                .setMaster("yarn")
                .setDeployMode("client");
        sparkLauncher.addAppArgs(new String[]{
                authenticationType,
                user,
                secureid,
                securekey,
                clusterShimInfo.getConfDir(),
                this.keytab,
                this.principal,
                detailId,
                sparkQueueName,
                record.getDatabaseName(),
                scriptContent
        });

        if (record.getDatabaseType().equalsIgnoreCase("iceberg")){
            String catalogType;
            final Map<String, String> datasource = getDatasource(user, record.getDatasourceId());
            if (datasource.containsKey("catalog-type")){
                catalogType = datasource.get("catalog-type");
            }else {
                catalogType = "hive";
            }
            final IcebergCatalogType fromType = IcebergCatalogType.getFromType(catalogType);
            fromType.getIcebergConf(datasource).forEach((key, value) -> sparkLauncher.setConf(key, value));
        }
        sparkLauncher.setSparkHome(clusterShimInfo.getBaseDir() + "/lib/spark");
        sparkLauncher.setVerbose(false);
        sparkLauncher.startApplication(new SparkAppHandle.Listener[0]);
        return null;
    }
}
