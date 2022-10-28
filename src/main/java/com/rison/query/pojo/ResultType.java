package com.rison.query.pojo;
import org.apache.commons.lang3.StringUtils;
/**
 * @PACKAGE_NAME: com.rison.query.handler
 * @NAME: ResultType
 * @USER: Rison
 * @DATE: 2022/10/28 14:57
 * @PROJECT_NAME: bigdata-query-handler
 **/



public enum ResultType {
    RESULT_SET("ResultSet", "the result is ResultSet"),
    UPDATE_SIZE("UpdateSize", "indicate how many rows were updated"),
    SPARK_DATASET("SparkDataSet", "DataSet in SparkSQL"),
    NONE("NONE", "no result for the sql");

    private String name;

    private String desc;

    ResultType(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public String getDesc() {
        return this.desc;
    }

    public static ResultType getByName(String name) {
        if (StringUtils.isNotBlank(name))
            for (ResultType type : values()) {
                if (type.getName().equalsIgnoreCase(name))
                    return type;
            }
        return NONE;
    }
}
