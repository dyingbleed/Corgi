package com.dyingbleed.corgi.web.bean;

import com.dyingbleed.corgi.core.bean.ODSTask;
import lombok.Data;

/**
 * Created by 李震 on 2019/3/29.
 */
@Deprecated
@Data
public class BatchTask {

    private Long id;

    private String name;

    private Long dataSourceId;

    private String dataSourceUrl;

    private String dataSourceUsername;

    private String dataSourcePassword;

    private String sourceDb;

    private String sourceTable;

    private String mode;

    private String timeColumn;

    private String sinkDb;

    private String sinkTable;

    public static BatchTask fromODSTask(ODSTask task) {
        BatchTask batchTask = new BatchTask();

        batchTask.setId(task.getId());
        batchTask.setName(task.getName());
        batchTask.setDataSourceId(task.getDatasourceId());
        batchTask.setDataSourceUrl(task.getDatasourceUrl());
        batchTask.setDataSourceUsername(task.getDatasourceUsername());
        batchTask.setDataSourcePassword(task.getDatasourcePassword());
        batchTask.setSourceDb(task.getSourceDb());
        batchTask.setSourceTable(task.getSourceTable());
        batchTask.setMode(task.getMode());
        batchTask.setTimeColumn(task.getTimeColumn());
        batchTask.setSinkDb(task.getSinkDb());
        batchTask.setSinkTable(task.getSinkTable());

        return batchTask;
    }

}
