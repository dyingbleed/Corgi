package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.core.bean.ODSTask;

import java.util.List;

/**
 * Created by 李震 on 2019/2/2.
 */
public interface ODSTaskService {

    /**
     * 根据 ID 删除 ODS 任务
     *
     * @param id 批量任务 ID
     *
     * */
    public void deleteODSTaskById(Long id);

    /**
     * 修改 ODS 任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    public void insertOrUpdateODSTask(ODSTask task);

    /**
     * 查询所有 ODS 任务
     *
     * @return 批量任务列表
     *
     * */
    public List<ODSTask> queryAllODSTask();

    /**
     * 根据 ID 查询 ODS 任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    public ODSTask queryODSTaskById(Long id);

    /**
     * 根据名称查询 ODS 任务
     *
     * @param name 批量任务名称
     *
     * @return 批量任务
     *
     * */
    public ODSTask queryODSTaskByName(String name);

}
