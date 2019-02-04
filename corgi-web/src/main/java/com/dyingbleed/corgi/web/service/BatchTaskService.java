package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.BatchTask;

import java.util.List;

/**
 * Created by 李震 on 2019/2/2.
 */
public interface BatchTaskService {

    /**
     * 新增批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    public BatchTask insertBatchTask(BatchTask task);

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    public void deleteBatchTaskById(Long id);

    /**
     * 修改批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    public BatchTask updateBatchTask(BatchTask task);

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    public List<BatchTask> queryAllBatchTask();

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    public BatchTask queryBatchTaskById(Long id);

    /**
     * 根据名称查询批量任务
     *
     * @param name 批量任务名称
     *
     * @return 批量任务
     *
     * */
    public BatchTask queryBatchTaskByName(String name);

}
