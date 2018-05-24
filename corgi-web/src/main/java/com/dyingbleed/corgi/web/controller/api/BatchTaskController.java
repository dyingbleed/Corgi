package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.service.BatchTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/15.
 */
@RestController
@RequestMapping("/api/batch")
public class BatchTaskController {

    private static Logger logger = LoggerFactory.getLogger(BatchTaskController.class);

    @Autowired
    private BatchTaskService batchTaskService;


    /**
     * 新增批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    @RequestMapping(method = RequestMethod.PUT)
    public BatchTask insertBatchTask(BatchTask task) {
        return this.batchTaskService.insertBatchTask(task);
    }

    /**
     * 根据 ID 删除批量任务
     *
     * @param id 批量任务 ID
     *
     * */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteBatchTaskById(@PathVariable("id") Long id) {
        this.batchTaskService.deleteBatchTaskById(id);
    }

    /**
     * 修改批量任务
     *
     * @param task 批量任务
     *
     * @return 批量任务
     *
     * */
    @RequestMapping(method = RequestMethod.POST)
    public BatchTask updateBatchTask(BatchTask task) {
        return this.batchTaskService.updateBatchTask(task);
    }

    /**
     * 查询所有批量任务
     *
     * @return 批量任务列表
     *
     * */
    @RequestMapping(method = RequestMethod.GET)
    public List<BatchTask> queryAllBatchTask(@RequestParam(value = "name", required = false) String name) {
        return this.batchTaskService.queryAllBatchTask();
    }

    /**
     * 根据 ID 查询批量任务
     *
     * @param id 批量任务 ID
     *
     * @return 批量任务
     *
     * */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public BatchTask queryBatchTaskById(@PathVariable("id") Long id) {
        return this.batchTaskService.queryBatchTaskById(id);
    }

}
