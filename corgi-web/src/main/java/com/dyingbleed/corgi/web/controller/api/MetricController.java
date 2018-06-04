package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import com.dyingbleed.corgi.web.service.MetricService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * 任务指标接口
 *
 * Created by 李震 on 2018/5/24.
 */
@RestController
@RequestMapping("/api/metric")
public class MetricController {

    @Autowired
    private MetricService metricService;

    /**
     * 新建任务指标
     *
     * @param metric 任务指标
     *
     * */
    @RequestMapping(method = RequestMethod.PUT)
    public void insertBatchTaskMetric(BatchTaskMetric metric) {
        this.metricService.insertBatchTaskMetric(metric);
    }

    /**
     * 根据批处理任务名称查询上一次执行时间
     *
     * @param name 批处理任务名
     *
     * */
    @RequestMapping(value = "/last", method = RequestMethod.GET)
    public BatchTaskMetric queryLastMetricByName(@RequestParam("name") String name) {
        return this.metricService.queryLastMetricByName(name);
    }

}
