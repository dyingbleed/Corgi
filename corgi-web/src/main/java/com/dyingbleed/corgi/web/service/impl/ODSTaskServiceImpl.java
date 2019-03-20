package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.mapper.ODSTaskMapper;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * ODS 任务
 *
 * Created by 李震 on 2018/5/15.
 */
@Service
public class ODSTaskServiceImpl implements ODSTaskService {

    @Autowired
    private ODSTaskMapper odsTaskMapper;

    @Override
    @Transactional
    public void deleteODSTaskById(Long id) {
        this.odsTaskMapper.deleteODSTaskById(id);
    }

    @Override
    @Transactional
    public void insertOrUpdateODSTask(ODSTask task) {
        if (task.getId() == null) {
            this.odsTaskMapper.insertODSTask(task);
        } else {
            this.odsTaskMapper.updateODSTask(task);
        }
    }

    @Override
    public List<ODSTask> queryAllODSTask() {
        return this.odsTaskMapper.queryAllODSTask();
    }

    @Override
    public ODSTask queryODSTaskById(Long id) {
        return this.odsTaskMapper.queryODSTaskById(id);
    }

    @Override
    public ODSTask queryODSTaskByName(String name) {
        return this.odsTaskMapper.queryODSTaskByName(name);
    }

}
