package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.core.bean.ODSTask;
import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.service.ODSTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by 李震 on 2019/3/13.
 */
@Deprecated
@RestController
@RequestMapping("/api/conf")
public class ConfController {

    @Autowired
    private ODSTaskService odsTaskService;

    /**
     * 根据名称获 ODS 配置
     *
     * @param name 批处理名称
     *
     * */
    @GetMapping
    public BatchTask getODSTaskConf(@RequestParam("name") String name) {
        ODSTask odsTask = this.odsTaskService.queryODSTaskByName(name);
        BatchTask batchTask = BatchTask.fromODSTask(odsTask);
        return batchTask;
    }

}
