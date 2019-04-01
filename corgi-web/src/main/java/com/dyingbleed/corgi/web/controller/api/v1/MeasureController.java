package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;
import com.dyingbleed.corgi.web.service.MeasureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 任务指标接口
 *
 * Created by 李震 on 2018/5/24.
 */
@RestController
@RequestMapping({"/api/measure", "/api/v1/measure"})
public class MeasureController {

    @Autowired
    private MeasureService measureService;

    /**
     * 新建任务指标
     *
     * @param measure 任务指标
     *
     * */
    @PutMapping
    public void insertMeasure(Measure measure) {
        this.measureService.insertMeasure(measure);
    }

    /**
     * 查询今日任务指标
     *
     * */
    @GetMapping("/stat")
    public MeasureStat queryTodayMeasureStat() {
        return this.measureService.queryTodayMeasureStat();
    }

    /**
     * 查询今日任务明细
     *
     * */
    @GetMapping("/detail")
    public List<Measure> queryTodayMeasureDetail() {
        return this.measureService.queryTodayMeasureDetail();
    }

}
