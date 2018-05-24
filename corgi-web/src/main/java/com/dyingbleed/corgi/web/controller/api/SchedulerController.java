package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.ScheduledTask;
import com.dyingbleed.corgi.web.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by 李震 on 2018/5/18.
 */
@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    @Autowired
    private SchedulerService schedulerService;

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public void scheduleTask(@PathVariable("id") Long id, ScheduledTask task) {
        this.schedulerService.scheduleTask(id, task.getStart_time(), task.getEnd_time(), task.getSchedule_time());
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void unscheduleTask(@PathVariable("id") Long id) {
        this.schedulerService.unscheduleTask(id);
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<ScheduledTask> getAllScheduledTask() {
        return this.schedulerService.getAllScheduledTask();
    }


}
