package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.mapper.BatchTaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by 李震 on 2018/5/15.
 */
@Service
public class BatchTaskService {

    private static Logger logger = LoggerFactory.getLogger(BatchTaskService.class);

    @Autowired
    private BatchTaskMapper batchTaskMapper;

    /**
     * 新增批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    public BatchTask insertBatchTask(BatchTask task) {
        this.batchTaskMapper.insertBatchTask(task);
        return task;
    }

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    public void deleteBatchTaskById(Long id) {
        this.batchTaskMapper.deleteBatchTaskById(id);
    }

    /**
     * 修改批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    public BatchTask updateBatchTask(BatchTask task) {
        this.batchTaskMapper.updateBatchTask(task);
        return task;
    }

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    public List<BatchTask> queryAllBatchTask() {
        return this.batchTaskMapper.queryAllBatchTask();
    }

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    public BatchTask queryBatchTaskById(Long id) {
        return this.batchTaskMapper.queryBatchTaskById(id);
    }

}
