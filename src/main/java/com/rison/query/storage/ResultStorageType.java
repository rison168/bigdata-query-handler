package com.rison.query.storage;

import com.rison.query.utils.DatastudioContextHolder;
import org.apache.commons.lang3.StringUtils;

/**
 * @PACKAGE_NAME: com.rison.query.storage
 * @NAME: ResultStorageType
 * @USER: Rison
 * @DATE: 2022/10/29 0:41
 * @PROJECT_NAME: bigdata-query-handler
 **/
public enum ResultStorageType {
    LOCAL("LOCAL", "本地存储", (Class)LocalResultStorage.class),
    HDFS("HDFS", "HDFS存储", (Class)HdfsResultStorage.class);

    private final Class<? extends ResultStorage> storageClass;

    private final String desc;

    private final String type;

    ResultStorageType(String type, String desc, Class<? extends ResultStorage> storageClass) {
        this.type = type;
        this.desc = desc;
        this.storageClass = storageClass;
    }

    public String getType() {
        return this.type;
    }

    public String getDesc() {
        return this.desc;
    }

    public Class<? extends ResultStorage> getStorageClass() {
        return this.storageClass;
    }

    public static ResultStorage getStorageBean(String type) {
        if (StringUtils.isNotBlank(type)) {
            String typeUpper = type.toUpperCase();
            for (ResultStorageType storageType : values()) {
                if (storageType.getType().equals(typeUpper))
                    return (ResultStorage)DatastudioContextHolder.getBean(storageType.getStorageClass());
            }
        }
        return (ResultStorage) DatastudioContextHolder.getBean(LOCAL.storageClass);
    }
    }
