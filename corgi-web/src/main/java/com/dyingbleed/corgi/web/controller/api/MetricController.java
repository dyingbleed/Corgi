package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.BatchTaskMetric;
import com.dyingbleed.corgi.web.service.MetricService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by 李震 on 2018/5/24.
 */
@RestController
@RequestMapping("/api/metric")
public class MetricController {

    @Autowired
    private MetricService metricService;

    @RequestMapping(method = RequestMethod.PUT)
    public void insertBatchTaskMetric(BatchTaskMetric metric) {
        this.metricService.insertBatchTaskMetric(metric);
    }

    @RequestMapping(method = RequestMethod.GET)
    public BatchTaskMetric queryLastBatchTaskMetricByName(@RequestParam("name") String name) {
        return this.metricService.queryLastBatchTaskMetricByName(name);
    }

}
