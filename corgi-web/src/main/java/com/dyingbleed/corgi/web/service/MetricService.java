package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import com.dyingbleed.corgi.web.mapper.MetricMapper;
import org.joda.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by 李震 on 2018/5/24.
 */
@Service
public class MetricService {

    @Autowired
    private MetricMapper metricMapper;

    public void insertBatchTaskMetric(BatchTaskMetric metric) {
        this.metricMapper.insertBatchTaskMetric(metric);
    }

    public BatchTaskMetric queryLastBatchTaskMetricByName(String name) {
        BatchTaskMetric metric = this.metricMapper.queryLastBatchTaskMetricByName(name);
        if (metric == null) return new BatchTaskMetric(0l, name, LocalDateTime.now().minusDays(1).withTime(0, 0, 0, 0).toDate());
        else return metric;
    }

}
