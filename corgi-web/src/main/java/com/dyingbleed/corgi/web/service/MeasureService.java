package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.bean.ExecuteLog;
import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;
import com.dyingbleed.corgi.web.func.Sync;
import com.dyingbleed.corgi.web.mapper.BatchTaskMapper;
import com.dyingbleed.corgi.web.mapper.MeasureMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 任务指标
 *
 * Created by 李震 on 2018/5/24.
 */
@Service
public class MeasureService {

    @Autowired
    private MeasureMapper measureMapper;

    @Autowired
    private BatchTaskMapper batchTaskMapper;

    @Autowired
    private Sync sync;

    /**
     * 新建任务指标
     *
     * @param measure 任务指标
     *
     * */
    public void insertMeasure(Measure measure) {
        this.measureMapper.insertMeasure(measure);
    }

    /**
     * 查询今日任务指标
     *
     * */
    public MeasureStat queryTodayMeasureStat() {
        return this.measureMapper.queryTodayMeasureStat();
    }

    /**
     * 查询今日任务明细
     *
     * */
    public List<Measure> queryTodayMeasureDetail() {
        return this.measureMapper.queryTodayMeasureDetail();
    }

    /**
     * 新建任务指标
     *
     * @param log 任务指标
     *
     * */
    public void insertExecuteLog(ExecuteLog log) {
        this.measureMapper.insertExecuteLog(log);

        BatchTask task = this.batchTaskMapper.queryBatchTaskByName(log.getBatchTaskName());
        checkNotNull(task);
        if (task.getSync()) this.sync.syncBatchTaskIncrementally(task); // 增量同步
    }

    /**
     * 根据批处理任务名称查询上一次执行时间
     *
     * @param name 批处理任务名
     *
     * */
    public ExecuteLog queryLastExecuteLogByName(String name) {
        return this.measureMapper.queryLastExecuteLogByName(name);
    }

}
