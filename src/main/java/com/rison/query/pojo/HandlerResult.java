package com.rison.query.pojo;

import com.rison.query.pojo.ResultType;

/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: HandlerResult
 * @USER: Rison
 * @DATE: 2022/10/28 14:56
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class HandlerResult {
    private Object resultSet;

    private Integer updateSize;

    private ResultType type;

    public Object getResultSet() {
        return this.resultSet;
    }

    public void setResultSet(Object resultSet) {
        this.resultSet = resultSet;
    }

    public Integer getUpdateSize() {
        return this.updateSize;
    }

    public void setUpdateSize(Integer updateSize) {
        this.updateSize = updateSize;
    }

    public ResultType getType() {
        return this.type;
    }

    public void setType(ResultType type) {
        this.type = type;
    }
}
