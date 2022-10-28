package com.rison.query.feign;

import com.tencent.tbds.cluster.client.ClusterClient;
import com.tencent.tbds.cluster.client.conf.ClusterClientConfig;
import com.tencent.tbds.cluster.config.common.ServerType;
import com.tencent.tbds.cluster.feign.api.ClusterFeignClient;
import com.tencent.tbds.cluster.feign.api.common.CallResult;
import com.tencent.tbds.cluster.feign.api.vo.FairQueueClusterVO;
import com.tencent.tbds.cluster.feign.api.vo.ShimInfoVO;
import com.tencent.tbds.datastudio.exception.BizException;
import com.tencent.tbds.datastudio.exception.InvokeRemoteServerException;
import com.tencent.tbds.datastudio.feign.TbdsPortalClient;
import com.tencent.tbds.datastudio.pojo.Authorization;
import com.tencent.tbds.datastudio.utils.AccessUtil;
import com.tencent.tbds.datastudio.utils.AuthorizationUtils;
import com.tencent.tbds.rpc.meta.ClusterInfo;
import com.tencent.tbds.rpc.meta.ShimInfo;
import com.tencent.tbds.tbdsportal.feign.api.vo.ProjectVO;
import com.tencent.tbds.tbdsportal.feign.api.vo.UserInfoVO;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.transaction.SystemException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
/**
 * @PACKAGE_NAME: com.rison.query.feign
 * @NAME: ClusterManagerClient
 * @USER: Rison
 * @DATE: 2022/10/29 0:58
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class ClusterManagerClient {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManagerClient.class);

    @Autowired
    private TbdsPortalClient tbdsPortalClient;

    @Autowired
    private AuthorizationUtils authorizationUtils;

    @Autowired
    private ClusterFeignClient clusterFeignClient;

    public static List<ClusterInfo> listClusters() throws Exception {
        return ClusterClient.getInstance(null).getEnableClusters();
    }

    public static String getDefaultClusterInfoId() throws Exception {
        return ClusterClient.getInstance(null).getDefaultClusterInfo().getId();
    }

    public static ClusterInfo getClusterInfoByIdentification(String clusterIdentification) throws Exception {
        for (ClusterInfo clusterInfo : listClusters()) {
            if (clusterInfo.getIdentification().equalsIgnoreCase(clusterIdentification))
                return clusterInfo;
        }
        return null;
    }

    public static ClusterInfo getClusterInfo(String clusterId) throws Exception {
        LOG.debug("begin to get cluster type by clusterId={}", clusterId);
        ClusterClientConfig config = new ClusterClientConfig();
        config.setClusterId(clusterId);
        ClusterClient client = ClusterClient.getInstance(config);
        ClusterInfo clusterInfo = client.getClusterInfo();
        LOG.debug("get result of cluster={}, by clusterId={}", clusterInfo, clusterId);
        return clusterInfo;
    }

    public static Map<String, String> getClusterMap() throws Exception {
        Map<String, String> clusterId2NameMap = new HashMap<>();
        List<ClusterInfo> clusterInfos = listClusters();
        if (clusterInfos != null) {
            for (ClusterInfo cluster : clusterInfos) {
                LOG.debug("cluster: {}", cluster);
                clusterId2NameMap.put(cluster.getId(), cluster.getDisplayName());
            }
        } else {
            LOG.warn("clusterInfos is null");
        }
        return clusterId2NameMap;
    }

    public Collection<ClusterInfo> getUserClusters(String userName) throws Exception {
        Map<String, ClusterInfo> id2Cluster = new HashMap<>();
        UserInfoVO user = this.tbdsPortalClient.getUserInfoVoByUserNameAndCheck(userName);
        if (user != null) {
            Set<ProjectVO> projects = this.tbdsPortalClient.getProjectsVoByUserId(user.getId().intValue());
            for (ProjectVO project : projects) {
                ClusterClientConfig config = new ClusterClientConfig();
                config.setProjectId(project.getId() + "");
                List<ClusterInfo> projectClusters = ClusterClient.getInstance(config).getClusterInfoByProjectId();
                if (projectClusters != null)
                    for (ClusterInfo cluster : projectClusters)
                        id2Cluster.put(cluster.getId(), cluster);
            }
        }
        return id2Cluster.values();
    }

    public static Collection<ClusterInfo> getProjectClusters(String projectId) throws Exception {
        Set<ClusterInfo> clusterInfos = new HashSet<>();
        ClusterClientConfig config = new ClusterClientConfig();
        config.setProjectId(projectId);
        List<ClusterInfo> projectClusters = ClusterClient.getInstance(config).getClusterInfoByProjectId();
        if (projectClusters != null)
            clusterInfos.addAll(projectClusters);
        return clusterInfos;
    }

    public Configuration getConfiguration(String userName) throws BizException, SystemException {
        UserInfoVO userInfoVO = null;
        try {
            userInfoVO = this.tbdsPortalClient.getUserInfoVoByUserNameAndCheck(userName);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new BizException(String.format("get user info failed, userName=%s", new Object[] { userName }));
        }
        return getConfiguration(userInfoVO.getId().intValue());
    }

    public Configuration getConfiguration(String userName, String clusterId) throws BizException, SystemException {
        UserInfoVO userInfoVO = null;
        try {
            userInfoVO = this.tbdsPortalClient.getUserInfoVoByUserNameAndCheck(userName);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new BizException(String.format("get user info failed, userName=%s", new Object[] { userName }));
        }
        return getConfiguration(userInfoVO.getId().intValue(), clusterId);
    }

    public Properties getRangerProperties(String userName, String clusterId) throws BizException, SystemException {
        UserInfoVO userInfoVO = null;
        try {
            userInfoVO = this.tbdsPortalClient.getUserInfoVoByUserNameAndCheck(userName);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new BizException(String.format("get user info failed, userName=%s", new Object[] { userName }));
        }
        String authorization = "";
        try {
            authorization = this.authorizationUtils.getPortalUserAuthorization(userInfoVO.getId().intValue());
        } catch (InvokeRemoteServerException e) {
            LOG.error(e.getMessage());
            throw new SystemException(String.format("get authorization failed, userId=%s", new Object[] { userInfoVO.getId() }));
        }
        CallResult<Properties> shimInfo = this.clusterFeignClient.getServerProperties(authorization, clusterId, ServerType.RANGER
                .name());
        if (shimInfo == null || !shimInfo.getResultCode().equals("0"))
            throw new SystemException(String.format("get conf failed, userId=%d", new Object[] { userInfoVO.getId() }));
        Properties properties = (Properties)shimInfo.getResultData();
        return properties;
    }

    public Configuration getConfiguration(int userId) throws SystemException {
        String authorization = "";
        try {
            authorization = this.authorizationUtils.getPortalUserAuthorization(userId);
        } catch (InvokeRemoteServerException e) {
            LOG.error(e.getMessage());
            throw new SystemException(String.format("get authorization failed, userId=%s", new Object[] { Integer.valueOf(userId) }));
        }
        CallResult<ShimInfoVO> shimInfo = this.clusterFeignClient.defaultClusterShimInfo(authorization);
        if (shimInfo == null || !shimInfo.getResultCode().equals("0"))
            throw new SystemException(String.format("get conf failed, userId=%d", new Object[] { Integer.valueOf(userId) }));
        ShimInfoVO shimInfoVO = (ShimInfoVO)shimInfo.getResultData();
        String hdfsConfDir = shimInfoVO.getConfDir() + "/hdfs";
        LOG.info("[getConfiguration] hdfsConfDir={}", hdfsConfDir);
        Configuration conf = new Configuration();
        conf.addResource(new Path(hdfsConfDir + "/core-site.xml"));
        conf.addResource(new Path(hdfsConfDir + "/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    public Configuration getConfiguration(int userId, String clusterId) throws SystemException {
        String authorization = "";
        try {
            authorization = this.authorizationUtils.getPortalUserAuthorization(userId);
        } catch (InvokeRemoteServerException e) {
            LOG.error(e.getMessage());
            throw new SystemException(String.format("get authorization failed, userId=%s", new Object[] { Integer.valueOf(userId) }));
        }
        CallResult<ShimInfoVO> shimInfo = this.clusterFeignClient.clusterShimInfo(authorization, clusterId);
        if (shimInfo == null || !shimInfo.getResultCode().equals("0"))
            throw new SystemException(String.format("get conf failed, userId=%d", new Object[] { Integer.valueOf(userId) }));
        ShimInfoVO shimInfoVO = (ShimInfoVO)shimInfo.getResultData();
        String hdfsConfDir = shimInfoVO.getConfDir() + "/hdfs";
        LOG.info("[getConfiguration] hdfsConfDir={}", hdfsConfDir);
        Configuration conf = new Configuration();
        conf.addResource(new Path(hdfsConfDir + "/core-site.xml"));
        conf.addResource(new Path(hdfsConfDir + "/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    public ShimInfo getShimInfo(String clusterId) throws Exception {
        ClusterClientConfig config = new ClusterClientConfig();
        config.setClusterId(clusterId);
        ClusterClient client = ClusterClient.getInstance(config);
        return client.getClusterShimInfo();
    }

    public Configuration getConf(Authorization authorization, String authentication, String confDir) throws Exception {
        Configuration conf = new Configuration();
        if ("tbds".equalsIgnoreCase(authentication)) {
            conf.set("hadoop_security_authentication_tbds_username", authorization.getUserName());
            conf.set("hadoop_security_authentication_tbds_secureid", authorization.getSecureId());
            conf.set("hadoop_security_authentication_tbds_securekey", authorization.getSecureId());
        }
        conf.set("hadoop.security.authentication", authentication);
        conf.addResource(new Path(confDir + "/hdfs/hdfs-site.xml"));
        conf.addResource(new Path(confDir + "/hdfs/core-site.xml"));
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
        return conf;
    }

    public FairQueueClusterVO getQueue(String queueId, String secureId, String secureKey) throws Exception {
        String authHeader = AccessUtil.getAccessAuthHeader(secureId, secureKey);
        CallResult<FairQueueClusterVO> result = this.clusterFeignClient.queueInfo(authHeader, queueId);
        return (FairQueueClusterVO)result.getResultData();
    }
}