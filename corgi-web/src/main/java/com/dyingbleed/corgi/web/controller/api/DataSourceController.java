package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.DataSource;
import com.dyingbleed.corgi.web.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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
     * */
    @RequestMapping(method = RequestMethod.POST)
    public DataSource updateDataSource(DataSource ds) {
        return this.dataSourceService.updateDataSource(ds);
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
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
     * */
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String testDataSourceConnection(DataSource ds) {
        return this.dataSourceService.testDataSourceConnection(ds);
    }

}
