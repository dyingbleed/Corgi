package com.dyingbleed.corgi.web.bean;

import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

/**
 * Created by 李震 on 2018/5/24.
 */
public class BatchTaskMetric {

    private Long id;

    private String batchTaskName;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date executeTime;

    public BatchTaskMetric() { super(); }

    public BatchTaskMetric(Long id, String batch_task_name, Date execute_time) {
        super();

        this.id = id;
        this.batchTaskName = batch_task_name;
        this.executeTime = execute_time;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBatchTaskName() {
        return batchTaskName;
    }

    public void setBatchTaskName(String batchTaskName) {
        this.batchTaskName = batchTaskName;
    }

    public Date getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Date executeTime) {
        this.executeTime = executeTime;
    }
}
