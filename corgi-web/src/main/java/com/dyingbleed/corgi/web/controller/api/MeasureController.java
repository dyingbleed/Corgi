package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.ExecuteLog;
import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;
import com.dyingbleed.corgi.web.service.MeasureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 任务指标接口
 *
 * Created by 李震 on 2018/5/24.
 */
@RestController
@RequestMapping("/api/measure")
public class MeasureController {

    @Autowired
    private MeasureService measureService;

    /**
     * 新建任务指标
     *
     * @param measure 任务指标
     *
     * */
    @RequestMapping(method = RequestMethod.PUT)
    public void insertMeasure(Measure measure) {
        this.measureService.insertMeasure(measure);
    }

    /**
     * 查询今日任务指标
     *
     * */
    @RequestMapping(value = "/stat", method = RequestMethod.GET)
    public MeasureStat queryTodayMeasureStat() {
        return this.measureService.queryTodayMeasureStat();
    }

    /**
     * 查询今日任务明细
     *
     * */
    @RequestMapping(value = "/detail", method = RequestMethod.GET)
    public List<Measure> queryTodayMeasureDetail() {
        return this.measureService.queryTodayMeasureDetail();
    }

    /**
     * 记录执行日志
     *
     * @param metric 任务指标
     *
     * */
    @RequestMapping(value = "/execute", method = RequestMethod.PUT)
    public void insertExecuteLog(ExecuteLog metric) {
        this.measureService.insertExecuteLog(metric);
    }

    /**
     * 根据批处理任务名称查询上一次执行时间
     *
     * @param name 批处理任务名
     *
     * */
    @RequestMapping(value = "/execute/last", method = RequestMethod.GET)
    public ExecuteLog queryLastExecuteLogByName(@RequestParam("name") String name) {
        return this.measureService.queryLastExecuteLogByName(name);
    }

}
