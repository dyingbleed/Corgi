package com.dyingbleed.corgi.web.job;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.service.BatchTaskService;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

/**
 * Created by 李震 on 2018/5/18.
 */
@Component
public class SparkJob extends QuartzJobBean {

    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);

    @Autowired
    private BatchTaskService batchTaskService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        Long id = context.getMergedJobDataMap().getLong("id");
        BatchTask batchTask = this.batchTaskService.queryBatchTaskById(id);
        logger.info(batchTask.toString());
    }
}
