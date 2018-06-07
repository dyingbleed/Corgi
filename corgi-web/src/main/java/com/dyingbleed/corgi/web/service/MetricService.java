package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import com.dyingbleed.corgi.web.mapper.MetricMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 任务指标
 *
 * Created by 李震 on 2018/5/24.
 */
@Service
public class MetricService {

    @Autowired
    private MetricMapper metricMapper;

    /**
     * 新建任务指标
     *
     * @param metric 任务指标
     *
     * */
    public void insertBatchTaskMetric(BatchTaskMetric metric) {
        this.metricMapper.insertBatchTaskMetric(metric);
    }

    /**
     * 根据批处理任务名称查询上一次执行时间
     *
     * @param name 批处理任务名
     *
     * */
    public BatchTaskMetric queryLastMetricByName(String name) {
        return this.metricMapper.queryLastMetricByName(name);
    }

}
