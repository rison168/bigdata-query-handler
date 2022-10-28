package com.rison.query.pojo;

import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

/**
 * @PACKAGE_NAME: com.rison.query.pojo
 * @NAME: QueryExecutionDetail
 * @USER: Rison
 * @DATE: 2022/10/28 15:47
 * @PROJECT_NAME: bigdata-query-handler
 **/
@Entity
@Table(name = "temporary_query_execution_detail")
public class QueryExecutionDetail implements Serializable {
    private Long id;

    private Long recordId;

    private String scriptContent;

    private String executor;

    private Date startTime;

    private Date endTime;

    private String status;

    private String resultType;

    private String resultPath;

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
    @Column(name = "record_id")
    public Long getRecordId() {
        return this.recordId;
    }

    public void setRecordId(Long recordId) {
        this.recordId = recordId;
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
    @Column(name = "end_time", columnDefinition = "TIMESTAMP(3) NULL DEFAULT NULL")
    public Date getEndTime() {
        return this.endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    @Basic
    @Column(name = "status")
    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Basic
    @Column(name = "result_type")
    public String getResultType() {
        return this.resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    @Basic
    @Column(name = "result_path")
    public String getResultPath() {
        return this.resultPath;
    }

    public void setResultPath(String resultPath) {
        this.resultPath = resultPath;
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
