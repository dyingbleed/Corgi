package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.core.bean.DMTask;
import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.service.DMTaskService;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by 李震 on 2019/3/13.
 */
@RestController
@RequestMapping({"/api/conf", "/api/v1/conf"})
public class ConfController {

    @Autowired
    private ODSTaskService odsTaskService;

    @Autowired
    private DMTaskService dmTaskService;

    /**
     * 根据名称获 ODS 配置
     *
     * @param name 批处理名称
     *
     * */
    @GetMapping("/ods")
    public ODSTask getODSTaskConf(@RequestParam("name") String name) {
        return this.odsTaskService.queryODSTaskByName(name);
    }

    /**
     * 根据名称获 DM 配置
     *
     * @param name 批处理名称
     *
     * */
    @GetMapping("/dm")
    public DMTask getDMTaskConf(@RequestParam("name") String name) {
        return this.dmTaskService.queryDMTaskByName(name);
    }

}
