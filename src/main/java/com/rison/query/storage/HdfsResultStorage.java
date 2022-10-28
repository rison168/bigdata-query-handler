package com.rison.query.storage;

import com.rison.query.feign.ClusterManagerClient;
import com.tencent.tbds.datastudio.security.TbdsLoginContext;
import com.tencent.tbds.datastudio.utils.HDFSUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;

/**
 * @PACKAGE_NAME: com.rison.query.storage
 * @NAME: HdfsResultStorage
 * @USER: Rison
 * @DATE: 2022/10/29 0:55
 * @PROJECT_NAME: bigdata-query-handler
 **/
@Component
public class HdfsResultStorage extends ResultStorage {
    private final Logger LOGGER = LoggerFactory.getLogger(HdfsResultStorage.class);

    @Autowired
    private TbdsLoginContext tbdsLoginContext;

    @Autowired
    private ClusterManagerClient clusterManagerClient;

    @Override
    public String upload(String content) {
        try {
            File resultFile = File.createTempFile("TempQuery", ".result", new File(this.queryProperties
                    .getLocalTmpDir()));
            FileWriter fileWriter = new FileWriter(resultFile);
            if (content == null) {
                fileWriter.write("null");
            } else {
                fileWriter.write(content);
            }
            fileWriter.flush();
            try {
                fileWriter.close();
            } catch (Exception exception) {}
            HDFSUtils.uploadFile(resultFile.getAbsolutePath(), resultFile.getAbsolutePath(), true);
            return resultFile.getAbsolutePath();
        } catch (Exception e) {
            this.LOGGER.error("error in store result to local filesystem", e);
            return null;
        }
    }

    @Override
    public String download(String path) {
        if (StringUtils.isNotBlank(path)) {
            try {
                if (HDFSUtils.fileExist(path).booleanValue()) {
                    HDFSUtils.downloadFile(path, path);
                    return new String(HDFSUtils.downloadFileWithStream(path), StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                this.LOGGER.error("error in downloading file[{}] from HDFS filesystem : ", path, e);
            }
        }
        this.LOGGER.warn("cannot get the file[{}] in HDFS filesystem", path);
        return null;
    }
}