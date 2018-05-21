package com.dyingbleed.corgi.web.bean;

import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

/**
 * Created by 李震 on 2018/5/18.
 */
public class ScheduledTask {

    private Long batch_task_id;

    private String name;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm")
    private LocalDateTime start_time;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm")
    private LocalDateTime end_time;

    @DateTimeFormat(pattern = "HH:mm")
    private LocalTime schedule_time;

    private Date nextFireTime;

    public Long getBatch_task_id() {
        return batch_task_id;
    }

    public void setBatch_task_id(Long batch_task_id) {
        this.batch_task_id = batch_task_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getStart_time() {
        return start_time;
    }

    public void setStart_time(LocalDateTime start_time) {
        this.start_time = start_time;
    }

    public LocalDateTime getEnd_time() {
        return end_time;
    }

    public void setEnd_time(LocalDateTime end_time) {
        this.end_time = end_time;
    }

    public LocalTime getSchedule_time() {
        return schedule_time;
    }

    public void setSchedule_time(LocalTime schedule_time) {
        this.schedule_time = schedule_time;
    }

    public Date getNextFireTime() {
        return nextFireTime;
    }

    public void setNextFireTime(Date nextFireTime) {
        this.nextFireTime = nextFireTime;
    }
}
