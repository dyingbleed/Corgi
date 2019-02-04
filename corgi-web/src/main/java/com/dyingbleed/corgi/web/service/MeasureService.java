package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;

import java.util.List;

/**
 * Created by 李震 on 2019/2/2.
 */
public interface MeasureService {

    /**
     * 新建任务指标
     *
     * @param measure 任务指标
     *
     * */
    public void insertMeasure(Measure measure);

    /**
     * 查询今日任务指标
     *
     * */
    public MeasureStat queryTodayMeasureStat();

    /**
     * 查询今日任务明细
     *
     * */
    public List<Measure> queryTodayMeasureDetail();

}
