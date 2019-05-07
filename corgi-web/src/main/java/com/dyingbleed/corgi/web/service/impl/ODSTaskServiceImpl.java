package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.bean.ODSTaskLog;
import com.dyingbleed.corgi.web.mapper.ODSTaskLogMapper;
import com.dyingbleed.corgi.web.mapper.ODSTaskMapper;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * ODS 任务
 *
 * Created by 李震 on 2018/5/15.
 */
@Service
@PropertySource({"file:${CORGI_HOME}/conf/application.yml", "file:${CORGI_HOME}/conf/cluster.properties"})
public class ODSTaskServiceImpl implements ODSTaskService {

    @Autowired
    private ODSTaskMapper odsTaskMapper;

    @Autowired
    private ODSTaskLogMapper odsTaskLogMapper;

    @Value("${livy.url}")
    private String livyUrl;

    @Value("${corgi.ods.path}")
    private String appPath;

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

    @Async
    @Override
    public void runODSTaskById(Long id) {
        ODSTask odsTask = queryODSTaskById(id);
        assert odsTask != null;
        runODSTask(odsTask);
    }

    @Async
    @Override
    public void runODSTaskByName(String name) {
        ODSTask odsTask = queryODSTaskByName(name);
        assert odsTask != null;
        runODSTask(odsTask);
    }

    private void runODSTask(ODSTask task) {
        throw new RuntimeException("not implement");
    }

    @Override
    public List<ODSTaskLog> queryODSTaskLogByTaskId(Long taskId) {
        return this.odsTaskLogMapper.queryODSTaskLogByTaskId(taskId);
    }
}
