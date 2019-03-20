package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.core.bean.DMTask;

import java.util.List;

/**
 * Created by 李震 on 2019/3/11.
 */
public interface DMTaskService {

    public void insertOrUpdateDMTask(DMTask task);

    public void deleteDMTaskById(Long id);

    public List<DMTask> queryAllDMTask();

    public DMTask queryDMTaskById(Long id);

    public DMTask queryDMTaskByName(String name);

}
