package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.core.bean.DMTask;
import com.dyingbleed.corgi.web.mapper.DMTaskMapper;
import com.dyingbleed.corgi.web.service.DMTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by 李震 on 2019/3/11.
 */
@Service
public class DMTaskServiceImpl implements DMTaskService {

    @Autowired
    private DMTaskMapper dmTaskMapper;

    @Override
    @Transactional
    public void insertOrUpdateDMTask(DMTask task) {
        if (null == task.getId()) {
            this.dmTaskMapper.insertDMTask(task);
        } else {
            this.dmTaskMapper.updateDMTask(task);
        }
    }

    @Override
    @Transactional
    public void deleteDMTaskById(Long id) {
        this.dmTaskMapper.deleteDMTaskById(id);
    }

    @Override
    public List<DMTask> queryAllDMTask() {
        return this.dmTaskMapper.queryAllDMTask();
    }

    @Override
    public DMTask queryDMTaskById(Long id) {
        return this.dmTaskMapper.queryDMTaskById(id);
    }

    @Override
    public DMTask queryDMTaskByName(String name) {
        return this.dmTaskMapper.queryDMTaskByName(name);
    }

}
