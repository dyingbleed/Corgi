package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.web.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Hive Metadata 接口
 *
 * Created by 李震 on 2018/5/17.
 */
@RestController
@RequestMapping({"/api/hive", "/api/v1/hive"})
public class HiveController {

    @Autowired
    private HiveService hiveService;

    /**
     * 显示 Hive 所有数据库
     *
     * @return 数据库列表
     *
     * */
    @GetMapping("/db")
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
    @GetMapping("/table/{db}")
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
    @GetMapping("/column/{db}/{table}")
    public List<Column> descTable(@PathVariable("db") String db, @PathVariable("table") String table) {
        return this.hiveService.descTable(db, table);
    }

}
