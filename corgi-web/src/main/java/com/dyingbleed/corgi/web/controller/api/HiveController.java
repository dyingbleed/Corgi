package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.Column;
import com.dyingbleed.corgi.web.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Hive Metadata 接口
 *
 * Created by 李震 on 2018/5/17.
 */
@RestController
@RequestMapping("/api/hive")
public class HiveController {

    @Autowired
    private HiveService hiveService;

    /**
     * 显示 Hive 所有数据库
     *
     * @return 数据库列表
     *
     * */
    @RequestMapping(value = "/db", method = RequestMethod.GET)
    public List<String> showDBs() {
        return this.hiveService.showDBs();
    }

    /**
     * 显示 Hive 数据库所有表
     *
     * @param db 数据库名
     *
     * @return 表列表
     *
     * */
    @RequestMapping(value = "/table/{db}", method = RequestMethod.GET)
    public List<String> showTables(@PathVariable("db") String db) {
        return this.hiveService.showTables(db);
    }

    /**
     * 显示 Hive 表所有字段
     *
     * @param db
     * @param table
     *
     * @return 表字段
     * */
    @RequestMapping(value = "/column/{db}/{table}", method = RequestMethod.GET)
    public List<Column> descTable(@PathVariable("db") String db, @PathVariable("table") String table) {
        return this.hiveService.descTable(db, table);
    }

}
