package com.rison.query.pojo;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;

import java.io.Serializable;
import java.util.Date;

/**
 * @PACKAGE_NAME: com.rison.query.pojo
 * @NAME: QueryExecutionRecord
 * @USER: Rison
 * @DATE: 2022/10/28 15:04
 * @PROJECT_NAME: bigdata-query-handler
 **/

@Entity
@Table(name = "query_execution_record")
public class QueryExecutionRecord implements Serializable {
    private Long id;

    private Long scriptId;

    private String scriptContent;

    private String executor;

    private String databaseType;

    private String datasourceId;

    private String databaseName;

    private String engineName;

    private String projectId;

    private Date startTime;

    private String createUser;

    private Date createTime;

    private String updateUser;

    private Date updateTime;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Basic
    @Column(name = "script_id")
    public Long getScriptId() {
        return this.scriptId;
    }

    public void setScriptId(Long scriptId) {
        this.scriptId = scriptId;
    }

    @Basic
    @Column(name = "script_content")
    public String getScriptContent() {
        return this.scriptContent;
    }

    public void setScriptContent(String scriptContent) {
        this.scriptContent = scriptContent;
    }

    @Basic
    @Column(name = "database_type")
    public String getDatabaseType() {
        return this.databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    @Basic
    @Column(name = "datasource_id")
    public String getDatasourceId() {
        return this.datasourceId;
    }

    public void setDatasourceId(String datasourceId) {
        this.datasourceId = datasourceId;
    }

    @Basic
    @Column(name = "database_name")
    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Basic
    @Column(name = "engine_name")
    public String getEngineName() {
        return this.engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    @Basic
    @Column(name = "project_id")
    public String getProjectId() {
        return this.projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    @Basic
    @Column(name = "executor")
    public String getExecutor() {
        return this.executor;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    @Basic
    @Column(name = "start_time", columnDefinition = "TIMESTAMP(3) NULL DEFAULT NULL")
    public Date getStartTime() {
        return this.startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    @Basic
    @Column(name = "create_user")
    public String getCreateUser() {
        return this.createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    @Basic
    @Column(name = "create_time", insertable = false, updatable = false, columnDefinition = "TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)")
    @Generated(GenerationTime.INSERT)
    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Basic
    @Column(name = "update_user")
    public String getUpdateUser() {
        return this.updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    @Basic
    @Column(name = "update_time", insertable = false, updatable = false, columnDefinition = "TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)")
    @Generated(GenerationTime.ALWAYS)
    public Date getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
