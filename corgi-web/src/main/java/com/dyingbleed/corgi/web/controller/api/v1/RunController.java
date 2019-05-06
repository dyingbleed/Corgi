package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.web.service.DMTaskService;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by 李震 on 2019-05-05.
 */
@RestController
@RequestMapping("/api/v1/run")
public class RunController {

    @Autowired
    private ODSTaskService odsTaskService;

    @Autowired
    private DMTaskService dmTaskService;

    /**
     * 根据名称运行 ODS 任务
     *
     * @param name 名称
     *
     * */
    @PostMapping("/ods")
    public void runODSTask(@RequestParam("name") String name) {
        this.odsTaskService.runODSTaskByName(name);
    }

    /**
     * 根据名称运行 DM 任务
     *
     * @param name 名称
     *
     * */
    @PostMapping("/dm")
    public void runDMTask(@RequestParam("name") String name) {
        this.dmTaskService.runDMTaskByName(name);
    }

}
