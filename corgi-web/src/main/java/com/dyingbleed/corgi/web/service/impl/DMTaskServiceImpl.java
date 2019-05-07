package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.core.bean.DMTask;
import com.dyingbleed.corgi.dm.LivyJob;
import com.dyingbleed.corgi.web.bean.DMTaskLog;
import com.dyingbleed.corgi.web.mapper.DMTaskLogMapper;
import com.dyingbleed.corgi.web.mapper.DMTaskMapper;
import com.dyingbleed.corgi.web.service.DMTaskService;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by 李震 on 2019/3/11.
 */
@Service
@PropertySource({"file:${CORGI_HOME}/conf/application.yml", "file:${CORGI_HOME}/conf/cluster.properties"})
public class DMTaskServiceImpl implements DMTaskService {

    private static final Logger logger = LoggerFactory.getLogger(DMTaskServiceImpl.class);

    @Autowired
    private DMTaskMapper dmTaskMapper;

    @Autowired
    private DMTaskLogMapper dmTaskLogMapper;

    @Value("${livy.url}")
    private String livyUrl;

    @Value("${corgi.dm.path}")
    private String appPath;

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

    @Async
    @Override
    public void runDMTaskById(Long id) {
        DMTask dmTask = queryDMTaskById(id);
        assert dmTask != null;
        runDMTask(dmTask);
    }

    @Async
    @Override
    public void runDMTaskByName(String name) {
        DMTask dmTask = queryDMTaskByName(name);
        assert dmTask != null;
        runDMTask(dmTask);
    }

    private void runDMTask(DMTask task) {
        LivyClient livyClient = null;
        try {
            livyClient = new LivyClientBuilder()
                    .setURI(new URI(this.livyUrl))
                    .build();

            File appFile = new File(appPath);
            assert appFile.exists();
            livyClient.uploadJar(appFile);

            livyClient.submit(new LivyJob(new String[]{task.getName()})).get();
        } catch (IOException | URISyntaxException | InterruptedException | ExecutionException e) {
            this.dmTaskLogMapper.insertDMTaskLog(DMTaskLog.failedLog(task.getId(), e)); // 记录失败日志

            throw new RuntimeException(e);
        } finally {
            if (livyClient != null) {
                livyClient.stop(true);
            }
        }

        this.dmTaskLogMapper.insertDMTaskLog(DMTaskLog.successLog(task.getId())); // 记录成功日志
    }

    @Override
    public List<DMTaskLog> queryDMTaskLogByTaskId(Long taskId) {
        return this.dmTaskLogMapper.queryDMTaskLogByTaskId(taskId);
    }


}
