package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.bean.ScheduledTask;
import com.dyingbleed.corgi.web.job.SparkJob;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 李震 on 2018/5/18.
 */
@Service
public class SchedulerService {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerService.class);

    @Autowired
    private BatchTaskService batchTaskService;

    @Autowired
    private Scheduler scheduler;

    public void scheduleTask(Long id, LocalDateTime startTime, LocalDateTime endTime, LocalTime scheduleTimeDaily) {
        BatchTask batchTask = this.batchTaskService.queryBatchTaskById(id);

        JobDetail jobDetail = JobBuilder.newJob(SparkJob.class)
                .withIdentity(batchTask.getName())
                .usingJobData("id", batchTask.getId())
                .build();

        CronScheduleBuilder schedule = CronScheduleBuilder.dailyAtHourAndMinute(scheduleTimeDaily.getHourOfDay(), scheduleTimeDaily.getMinuteOfHour())
                .withMisfireHandlingInstructionIgnoreMisfires();

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(batchTask.getName())
                .startAt(startTime.toDate())
                .endAt(endTime.toDate())
                .withSchedule(schedule)
                .build();

        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void unscheduleTask(Long id) {
        BatchTask batchTask = this.batchTaskService.queryBatchTaskById(id);

        try {
            this.scheduler.unscheduleJob(TriggerKey.triggerKey(batchTask.getName()));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public List<ScheduledTask> getAllScheduledTask() {
        List<ScheduledTask> tasks = new LinkedList<>();

        try {
            for (TriggerKey triggerKey :
                    this.scheduler.getTriggerKeys(GroupMatcher.anyGroup())) {
                Trigger trigger = this.scheduler.getTrigger(triggerKey);
                Date nextFireTime = trigger.getNextFireTime();

                JobKey jobKey = trigger.getJobKey();
                JobDetail jobDetail = this.scheduler.getJobDetail(jobKey);
                long taskId = jobDetail.getJobDataMap().getLong("id");

                ScheduledTask task = new ScheduledTask();
                task.setBatch_task_id(taskId);
                task.setName(triggerKey.getName());
                if (nextFireTime != null) task.setNextFireTime(nextFireTime);
                tasks.add(task);
            }
        } catch (SchedulerException e) {
            logger.error("获取调度任务失败", e);
        }

        return tasks;
    }

}
