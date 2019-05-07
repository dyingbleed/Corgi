package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.bean.ODSTaskLog;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 批处理任务接口
 *
 * Created by 李震 on 2018/5/15.
 */
@RestController
@RequestMapping({"/api/ods", "/api/v1/ods"})
public class ODSTaskController {

    private static Logger logger = LoggerFactory.getLogger(ODSTaskController.class);

    @Autowired
    private ODSTaskService odsTaskService;

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    @DeleteMapping("/{id}")
    public void deleteODSTaskById(@PathVariable("id") Long id) {
        this.odsTaskService.deleteODSTaskById(id);
    }

    /**
     * 修改批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    @PostMapping
    public void insertOrUpdateODSTask(ODSTask task) {
        this.odsTaskService.insertOrUpdateODSTask(task);
    }

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    @GetMapping
    public List<ODSTask> queryAllODSTask(@RequestParam(value = "name", required = false) String name) {
        return this.odsTaskService.queryAllODSTask();
    }

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    @GetMapping("/{id}")
    public ODSTask queryODSTaskById(@PathVariable("id") Long id) {
        return this.odsTaskService.queryODSTaskById(id);
    }

    @PostMapping("/run/{id}")
    public void runODSTaskById(@PathVariable("id") Long id) {
        this.odsTaskService.runODSTaskById(id);
    }

    @GetMapping("/log/{task_id}")
    public List<ODSTaskLog> queryODSTaskLogByTaskId(@PathVariable("task_id") Long taskId) {
        return this.odsTaskService.queryODSTaskLogByTaskId(taskId);
    }

}
