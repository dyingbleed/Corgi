package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.BatchTask;
import com.dyingbleed.corgi.web.service.BatchTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 配置接口
 *
 * Created by 李震 on 2018/5/24.
 */
@RestController
@RequestMapping("/api/conf")
public class ConfController {

    @Autowired
    private BatchTaskService batchTaskService;

    /**
     * 根据名称获取批处理
     *
     * @param name 批处理名称
     *
     * */
    @RequestMapping(method = RequestMethod.GET)
    public BatchTask queryBatchTaskByName(@RequestParam("name") String name) {
        return this.batchTaskService.queryBatchTaskByName(name);
    }

}
