package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.DataSource;
import com.dyingbleed.corgi.web.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 数据源接口
 *
 * Created by 李震 on 2018/4/12.
 */
@RestController
@RequestMapping("/api/datasource")
public class DataSourceController {

    @Autowired
    private DataSourceService dataSourceService;

    /**
     * 新增数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @RequestMapping(method = RequestMethod.PUT)
    public DataSource insertDataSource(DataSource ds) {
        return this.dataSourceService.insertDataSource(ds);
    }

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteDataSourceById(@PathVariable("id") Long id) {
        this.dataSourceService.deleteDataSourceById(id);
    }

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @RequestMapping(method = RequestMethod.POST)
    public DataSource updateDataSource(DataSource ds) {
        return this.dataSourceService.updateDataSource(ds);
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
     *
     * */
    @RequestMapping(method = RequestMethod.GET)
    public List<DataSource> queryAllDataSource() {
        return this.dataSourceService.queryAllDataSource();
    }

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     *
     * */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public DataSource queryDataSourceById(@PathVariable("id") Long id) {
        return this.dataSourceService.queryDataSourceById(id);
    }

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String testConnection(DataSource ds) {
        return this.dataSourceService.testConnection(ds);
    }

    /**
     * 显示所有数据库
     *
     * @param id 数据源 ID
     *
     * @return 数据库列表
     *
     * */
    @RequestMapping(value = "/db/{id}", method = RequestMethod.GET)
    public List<String> showDBs(@PathVariable("id") Long id) {
        return this.dataSourceService.showDBs(id);
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
    @RequestMapping(value = "/table/{id}/{db}", method = RequestMethod.GET)
    public List<String> showTables(@PathVariable("id") Long id, @PathVariable("db") String db) {
        return this.dataSourceService.showTables(id, db);
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
    @RequestMapping(value = "/timecolumn/{id}/{db}/{table}")
    public Map<String, String> getTimeColumns(@PathVariable("id") Long id, @PathVariable("db") String db, @PathVariable("table") String table) {
        return this.dataSourceService.getTimeColumns(id, db, table);
    }

}
