package com.dyingbleed.corgi.web.job;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.service.BatchTaskService;
import org.apache.spark.launcher.SparkLauncher;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by 李震 on 2018/5/18.
 */
@Component
public class SparkJob extends QuartzJobBean {

    private static final Logger logger = LoggerFactory.getLogger(SparkJob.class);

    @Value("${spark.spark_home}")
    private String sparkHome;

    @Value("${server.address}")
    private String serverAddress;

    @Value("${server.port}")
    private Integer port;

    @Autowired
    private BatchTaskService batchTaskService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        Long id = context.getMergedJobDataMap().getLong("id");
        BatchTask batchTask = this.batchTaskService.queryBatchTaskById(id);

        try {
            new SparkLauncher()
                    .setSparkHome(sparkHome)
                    .setAppResource("/Users/anthony/Git/Corgi/corgi-spark/target/corgi-spark")
                    .setMainClass("com.dyingbleed.spark.demo.test.Application")
                    .setMaster("local")
                    .addAppArgs(batchTask.getName(), serverAddress + ":" + port)
                    .startApplication();
        } catch (IOException e) {
            throw new JobExecutionException(e);
        }
    }
}
