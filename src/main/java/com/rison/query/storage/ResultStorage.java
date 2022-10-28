package com.rison.query.storage;

import com.rison.query.common.QueryProperties;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @PACKAGE_NAME: com.rison.query.storage
 * @NAME: ResultStorage
 * @USER: Rison
 * @DATE: 2022/10/29 0:45
 * @PROJECT_NAME: bigdata-query-handler
 **/
public abstract class ResultStorage {
    protected QueryProperties queryProperties;

    @Autowired
    public void setTempQueryProperties(QueryProperties queryProperties) {
        this.queryProperties = queryProperties;
    }

    public abstract String upload(String paramString);

    public abstract String download(String paramString);
}
