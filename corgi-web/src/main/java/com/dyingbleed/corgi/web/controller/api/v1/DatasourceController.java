package com.dyingbleed.corgi.web.controller.api.v1;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.web.bean.Datasource;
import com.dyingbleed.corgi.web.service.DatasourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 数据源接口
 *
 * Created by 李震 on 2018/4/12.
 */
@RestController
@RequestMapping({"/api/datasource", "/api/v1/datasource"})
public class DatasourceController {

    @Autowired
    private DatasourceService datasourceService;

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    @DeleteMapping("/{id}")
    public void deleteDatasourceById(@PathVariable("id") Long id) {
        this.datasourceService.deleteDatasourceById(id);
    }

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @PostMapping
    public void insertOrUpdateDatasource(Datasource ds) {
        this.datasourceService.insertOrUpdateDatasource(ds);
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
     *
     * */
    @GetMapping
    public List<Datasource> queryAllDatasource() {
        return this.datasourceService.queryAllDatasource();
    }

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     *
     * */
    @GetMapping("/{id}")
    public Datasource queryDatasourceById(@PathVariable("id") Long id) {
        return this.datasourceService.queryDatasourceById(id);
    }

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @GetMapping("/test")
    public String testConnection(Datasource ds) {
        return this.datasourceService.testConnection(ds);
    }

    /**
     * 显示所有数据库
     *
     * @param id 数据源 ID
     *
     * @return 数据库列表
     *
     * */
    @GetMapping("/db/{id}")
    public List<String> showDBs(@PathVariable("id") Long id) {
        return this.datasourceService.showDBs(id);
    }

    /**
     * 显示数据库所有表
     *
     * @param id 数据源 ID
     * @param db 数据库名
     *
     * @return 表列表
     *
     * */
    @GetMapping("/table/{id}/{db}")
    public List<String> showTables(@PathVariable("id") Long id, @PathVariable("db") String db) {
        return this.datasourceService.showTables(id, db);
    }

    /**
     * 显示所有字段
     *
     * @param id 数据源 ID
     * @param db 数据库名
     * @param table 表名
     *
     * @return 字段
     *
     * */
    @GetMapping("/column/{id}/{db}/{table}")
    public List<Column> descTable(@PathVariable("id") Long id, @PathVariable("db") String db, @PathVariable("table") String table) {
        return this.datasourceService.descTable(id, db, table);
    }

    /**
     * 显示所有修改时间字段
     *
     * @param id 数据源 ID
     * @param db 数据库名
     * @param table 表名
     *
     * @return 字段
     *
     * */
    @GetMapping("/timecolumn/{id}/{db}/{table}")
    public Map<String, String> getTimeColumns(@PathVariable("id") Long id, @PathVariable("db") String db, @PathVariable("table") String table) {
        return this.datasourceService.getTimeColumns(id, db, table);
    }

}
