package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.web.bean.Measure;
import com.dyingbleed.corgi.web.bean.MeasureStat;
import com.dyingbleed.corgi.web.mapper.MeasureMapper;
import com.dyingbleed.corgi.web.service.MeasureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 任务指标
 *
 * Created by 李震 on 2018/5/24.
 */
@Service
public class MeasureServiceImpl implements MeasureService {

    @Autowired
    private MeasureMapper measureMapper;

    /**
     * 新建任务指标
     *
     * @param measure 任务指标
     *
     * */
    @Override
    @Transactional
    public void insertMeasure(Measure measure) {
        this.measureMapper.insertMeasure(measure);
    }

    /**
     * 查询今日任务指标
     *
     * */
    @Override
    public MeasureStat queryTodayMeasureStat() {
        return this.measureMapper.queryTodayMeasureStat();
    }

    /**
     * 查询今日任务明细
     *
     * */
    @Override
    public List<Measure> queryTodayMeasureDetail() {
        return this.measureMapper.queryTodayMeasureDetail();
    }

}
