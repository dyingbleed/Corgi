package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.DataSource;
import com.dyingbleed.corgi.web.mapper.DataSourceMapper;
import com.dyingbleed.corgi.web.utils.JDBCUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by 李震 on 2018/5/9.
 */
@Service
public class DataSourceService {

    @Autowired
    private DataSourceMapper dataSourceMapper;

    /**
     * 新增数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     * */
    public DataSource insertDataSource(DataSource ds) {
        this.dataSourceMapper.insertDataSource(ds);
        return ds;
    }

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    public void deleteDataSourceById(Long id) {
        this.dataSourceMapper.deleteDataSourceById(id);
    }

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     * */
    public DataSource updateDataSource(DataSource ds) {
        this.dataSourceMapper.updateDataSource(ds);
        return ds;
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
     * */
    public List<DataSource> queryAllDataSource() {
        return this.dataSourceMapper.queryAllDataSource();
    }

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     * */
    public DataSource queryDataSourceById(Long id) {
        return this.dataSourceMapper.queryDataSourceById(id);
    }

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 结果
     * */
    public String testDataSourceConnection(DataSource ds) {
        String message = "success";
        try {
            JDBCUtils.testMySQLConnection(ds.getUrl(), ds.getUsername(), ds.getPassword());
        } catch (Exception e) {
            message = e.getLocalizedMessage();
        }
        return message;
    }

}
