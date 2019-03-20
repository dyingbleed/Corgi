package com.dyingbleed.corgi.web.job;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.Constants;
import com.dyingbleed.corgi.web.bean.Alert;
import com.dyingbleed.corgi.web.service.DatasourceService;
import com.dyingbleed.corgi.web.service.DetectorService;
import com.dyingbleed.corgi.web.service.HiveService;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by 李震 on 2019/2/15.
 */
@Component
public class SchemaDetectorJob {

    @Autowired
    private ODSTaskService odsTaskService;

    @Autowired
    private DatasourceService dataSourceService;

    @Autowired
    private HiveService hiveService;

    @Autowired
    private DetectorService detectorService;

    @Scheduled(fixedDelay = 1000 * 60 * 5)
    public void detect() {
        List<ODSTask> odsTasks = this.odsTaskService.queryAllODSTask();
        ODSTask odsTask = odsTasks.get(new Random().nextInt(odsTasks.size()));

        final List<Column> sourceColumns = dataSourceService.descTable(odsTask.getDatasourceId(), odsTask.getSourceDb(), odsTask.getSourceTable());
        final List<Column> sinkColumns = hiveService
                .descTable(odsTask.getSinkDb(), odsTask.getSinkTable())
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
            alert.setBatchTaskId(odsTask.getId());


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
            this.detectorService.deleteAlert(Constants.ALERT_TYPE_SCHEMA_NOT_MATCH, odsTask.getId());
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
    private final static Set<Integer> INTEGER_TYPE_SET = newHashSet(
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT
    );
    // 实数类型
    private final static Set<Integer> REAL_TYPE_SET = newHashSet(
            Types.NUMERIC,
            Types.REAL,
            Types.DECIMAL,
            Types.INTEGER,
            Types.FLOAT,
            Types.DOUBLE
    );
    // 字符类型
    private final static Set<Integer> STRING_TYPE_SET = newHashSet(
            Types.CHAR,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.LONGVARCHAR,
            Types.LONGNVARCHAR
    );
    // 日期类型
    private final static Set<Integer> DATE_TYPE_SET = newHashSet(
            Types.DATE,
            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE
    );
    // 布尔类型
    private final static Set<Integer> BIN_TYPE_SET = newHashSet(
            Types.BINARY,
            Types.BIT,
            Types.BOOLEAN,
            Types.VARBINARY
    );

    private static <E> HashSet<E> newHashSet(E... elements) {
        HashSet<E> set = new HashSet<>(elements.length);
        Collections.addAll(set, elements);
        return set;
    }

    private boolean columnEqual(Column sourceColumn, Column sinkColumn) {
        boolean isNameEqual = sourceColumn.getName().equalsIgnoreCase(sinkColumn.getName());

        boolean isDataTypeEqual;
        if (sourceColumn.getType() == sinkColumn.getType()) {
            isDataTypeEqual = true;
        } else {
            isDataTypeEqual = (INTEGER_TYPE_SET.contains(sourceColumn.getType()) && INTEGER_TYPE_SET.contains(sinkColumn.getType())) ||
                    (REAL_TYPE_SET.contains(sourceColumn.getType()) && REAL_TYPE_SET.contains(sinkColumn.getType())) ||
                    (STRING_TYPE_SET.contains(sourceColumn.getType()) && STRING_TYPE_SET.contains(sinkColumn.getType())) ||
                    (DATE_TYPE_SET.contains(sourceColumn.getType()) && DATE_TYPE_SET.contains(sinkColumn.getType())) ||
                    (BIN_TYPE_SET.contains(sourceColumn.getType()) && BIN_TYPE_SET.contains(sinkColumn.getType()));
        }

        return isNameEqual && isDataTypeEqual;
    }

}
