package com.dyingbleed.corgi.web.job;

import com.dyingbleed.corgi.web.Constants;
import com.dyingbleed.corgi.web.bean.Alert;
import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.bean.Column;
import com.dyingbleed.corgi.web.service.BatchTaskService;
import com.dyingbleed.corgi.web.service.DataSourceService;
import com.dyingbleed.corgi.web.service.DetectorService;
import com.dyingbleed.corgi.web.service.HiveService;
import com.dyingbleed.corgi.web.utils.JDBCUtils;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by 李震 on 2019/2/15.
 */
@Component
public class SchemaDetectorJob {

    @Autowired
    private BatchTaskService batchTaskService;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private HiveService hiveService;

    @Autowired
    private DetectorService detectorService;

    @Scheduled(fixedDelay = 1000 * 60 * 5)
    public void detect() {
        List<BatchTask> batchTasks = this.batchTaskService.queryAllBatchTask();
        BatchTask batchTask = batchTasks.get(new Random().nextInt(batchTasks.size()));

        final List<Column> sourceColumns = dataSourceService.descTable(batchTask.getDataSourceId(), batchTask.getSourceDb(), batchTask.getSourceTable());
        final List<Column> sinkColumns = hiveService
                .descTable(batchTask.getSinkDb(), batchTask.getSinkTable())
                .stream()
                .filter(c -> !c.getName().equalsIgnoreCase("ods_date"))
                .collect(Collectors.toList());

        ArrayList<Column> sourceDiffColumns = new ArrayList<>(sourceColumns);
        for (Column sourceColumn: sourceColumns) {
            for (Column sinkColumn: sinkColumns) {
                if (columnEqual(sourceColumn, sinkColumn)) {
                    sourceDiffColumns.remove(sourceColumn);
                }
            }
        }

        ArrayList<Column> sinkDiffColumns = new ArrayList<>(sinkColumns);
        for (Column sinkColumn: sinkColumns) {
            for (Column sourceColumn: sourceColumns) {
                if (columnEqual(sourceColumn, sinkColumn)) {
                    sinkDiffColumns.remove(sinkColumn);
                }
            }
        }

        if (sourceDiffColumns.size() != 0 || sinkDiffColumns.size() != 0) {
            Alert alert = new Alert();

            String msg;

            if (sourceDiffColumns.size() != 0 && sinkDiffColumns.size() == 0) {
                alert.setLevel(Constants.ALERT_LEVEL_WARN);
                msg = "⚠️";
            } else {
                alert.setLevel(Constants.ALERT_LEVEL_DANGER);
                msg = "❌";
            }
            alert.setType(Constants.ALERT_TYPE_SCHEMA_NOT_MATCH);
            alert.setBatchTaskId(batchTask.getId());


            if (sourceDiffColumns.size() != 0) {
                msg += " Source 不匹配字段：" + String.join(", ", sourceDiffColumns.stream()
                        .map(c -> c.getName() + " " + c.getTypeName())
                        .collect(Collectors.toList()));
            }
            if (sinkDiffColumns.size() != 0) {
                msg += " Sink 不匹配字段：" + String.join(", ", sinkDiffColumns.stream()
                        .map(c -> c.getName() + " " + c.getTypeName())
                        .collect(Collectors.toList()));
            }

            alert.setMsg(msg);

            this.detectorService.saveOrUpdateAlert(alert);
        } else {
            this.detectorService.deleteAlert(Constants.ALERT_TYPE_SCHEMA_NOT_MATCH, batchTask.getId());
        }
    }

    /**
     * 判断两个列是否相等
     *
     * 1. 列名相同
     * 2. 类型相同
     *
     * */
    // 整数类型
    private final static Set<Integer> INTEGER_TYPE_SET = Sets.newHashSet(
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT
    );
    // 实数类型
    private final static Set<Integer> REAL_TYPE_SET = Sets.newHashSet(
            Types.NUMERIC,
            Types.REAL,
            Types.DECIMAL,
            Types.INTEGER,
            Types.FLOAT,
            Types.DOUBLE
    );
    // 字符类型
    private final static Set<Integer> STRING_TYPE_SET = Sets.newHashSet(
            Types.CHAR,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.LONGVARCHAR,
            Types.LONGNVARCHAR
    );
    // 日期类型
    private final static Set<Integer> DATE_TYPE_SET = Sets.newHashSet(
            Types.DATE,
            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE
    );

    private boolean columnEqual(Column sourceColumn, Column sinkColumn) {
        boolean isNameEqual = sourceColumn.getName().equalsIgnoreCase(sinkColumn.getName());

        boolean isDataTypeEqual = false;
        if (sourceColumn.getDataType() == sinkColumn.getDataType()) {
            isDataTypeEqual = true;
        } else {
            if (INTEGER_TYPE_SET.contains(sourceColumn.getDataType()) && INTEGER_TYPE_SET.contains(sinkColumn.getDataType())) {
                isDataTypeEqual = true;
            } else if (REAL_TYPE_SET.contains(sourceColumn.getDataType()) && REAL_TYPE_SET.contains(sinkColumn.getDataType())) {
                isDataTypeEqual = true;
            } else if (STRING_TYPE_SET.contains(sourceColumn.getDataType()) && STRING_TYPE_SET.contains(sinkColumn.getDataType())) {
                isDataTypeEqual = true;
            } else if (DATE_TYPE_SET.contains(sourceColumn.getDataType()) && DATE_TYPE_SET.contains(sinkColumn.getDataType())) {
                isDataTypeEqual = true;
            }
        }

        return isNameEqual && isDataTypeEqual;
    }

}
