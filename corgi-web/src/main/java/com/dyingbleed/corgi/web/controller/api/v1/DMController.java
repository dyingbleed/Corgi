package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.core.bean.DMTask;
import com.dyingbleed.corgi.web.service.DMTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by 李震 on 2019/3/11.
 */
@RestController
@RequestMapping({"/api/dm", "/api/v1/dm"})
public class DMController {

    @Autowired
    private DMTaskService dmTaskService;

    @PostMapping
    public void insertOrUpdateDMTask(DMTask task) {
        this.dmTaskService.insertOrUpdateDMTask(task);
    }

    @DeleteMapping("/{id}")
    public void deleteDMTaskById(@PathVariable("id") Long id) {
        this.dmTaskService.deleteDMTaskById(id);
    }

    @GetMapping
    public List<DMTask> queryAllDMTask() {
        return this.dmTaskService.queryAllDMTask();
    }

    @GetMapping("/{id}")
    public DMTask queryDMTaskById(@PathVariable("id") Long id) {
        return this.dmTaskService.queryDMTaskById(id);
    }

    @PostMapping("/run/{id}")
    public void runDMTaskById(@PathVariable("id") Long id) {

    }

}
