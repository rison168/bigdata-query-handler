package com.rison.query.storage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @PACKAGE_NAME: com.rison.query.storage
 * @NAME: LocalResultStorage
 * @USER: Rison
 * @DATE: 2022/10/29 0:53
 * @PROJECT_NAME: bigdata-query-handler
 **/
@Component
public class LocalResultStorage extends ResultStorage {
    private final Logger LOGGER = LoggerFactory.getLogger(LocalResultStorage.class);

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
            return resultFile.getAbsolutePath();
        } catch (Exception e) {
            this.LOGGER.error("error in store result to local filesystem", e);
            return null;
        }
    }

    @Override
    public String download(String path) {
        if (StringUtils.isNotBlank(path)) {
            File file = new File(path);
            if (file.isFile())
                try {
                    return new String(Files.readAllBytes(Paths.get(path, new String[0])), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    this.LOGGER.error("error in reading file[{}]", path, e);
                }
        }
        this.LOGGER.warn("cannot find the file[{}] in LOCAL filesystem", path);
        return null;
    }
}
