package com.dyingbleed.corgi.web.bean;

import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

/**
 * Created by 李震 on 2018/5/24.
 */
public class BatchTaskMetric {

    private Long id;

    private String batch_task_name;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date execute_time;

    public BatchTaskMetric() { super(); }

    public BatchTaskMetric(Long id, String batch_task_name, Date execute_time) {
        super();

        this.id = id;
        this.batch_task_name = batch_task_name;
        this.execute_time = execute_time;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBatch_task_name() {
        return batch_task_name;
    }

    public void setBatch_task_name(String batch_task_name) {
        this.batch_task_name = batch_task_name;
    }

    public Date getExecute_time() {
        return execute_time;
    }

    public void setExecute_time(Date execute_time) {
        this.execute_time = execute_time;
    }
}
