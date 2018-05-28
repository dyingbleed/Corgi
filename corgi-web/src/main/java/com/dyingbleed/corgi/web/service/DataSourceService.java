package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.DataSource;
import com.dyingbleed.corgi.web.mapper.BatchTaskMapper;
import com.dyingbleed.corgi.web.mapper.DataSourceMapper;
import com.dyingbleed.corgi.web.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 数据源
 *
 * Created by 李震 on 2018/5/9.
 */
@Service
public class DataSourceService {

    private static Logger logger = LoggerFactory.getLogger(DataSourceService.class);

    @Autowired
    private DataSourceMapper dataSourceMapper;

    @Autowired
    private BatchTaskMapper batchTaskMapper;

    /**
     * 新增数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
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
        this.dataSourceMapper.deleteDataSourceById(id); // 删除 datasource
        this.batchTaskMapper.deleteBatchTaskByDataSourceId(id); // 删除 datasource 关联的 batch_task
    }

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    public DataSource updateDataSource(DataSource ds) {
        this.dataSourceMapper.updateDataSource(ds);
        return ds;
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
     *
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
     *
     * */
    public DataSource queryDataSourceById(Long id) {
        return this.dataSourceMapper.queryDataSourceById(id);
    }

    /**
     * 根据名称查询数据源
     *
     * @param name 数据源名称
     *
     * @return 数据源
     *
     * */
    public DataSource queryDataSourceByName(String name) {
        return this.dataSourceMapper.queryDataSourceByName(name);
    }

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 结果
     *
     * */
    public String testConnection(DataSource ds) {
        String message = "success";
        try {
            JDBCUtils.testMySQLConnection(ds.getUrl(), ds.getUsername(), ds.getPassword());
        } catch (Exception e) {
            message = e.getLocalizedMessage();
        }
        return message;
    }

    /**
     * 显示所有数据库
     *
     * @param id 数据源 ID
     *
     * @return 数据库列表
     *
     * */
    @Cacheable(cacheNames = "source_db")
    public List<String> showDBs(@PathVariable("id") Long id) {
        LinkedList<String> databases = new LinkedList<>();

        DataSource ds = this.queryDataSourceById(id);
        try {
            databases.addAll(JDBCUtils.showDatabases(ds.getUrl(), ds.getUsername(), ds.getPassword()));
        } catch (ClassNotFoundException | SQLException  e) {
            logger.error("显示所有数据库出错", e);
        }

        return databases;
    }

    /**
     * 显示数据库所有表
     *
     * @param id 数据源 ID
     * @param database 数据库名
     *
     * @return 表列表
     *
     * */
    @Cacheable(cacheNames = "source_table")
    public List<String> showTables(Long id, String database) {
        LinkedList<String> tables = new LinkedList<>();

        DataSource ds = this.queryDataSourceById(id);
        try {
            tables.addAll(JDBCUtils.showTables(ds.getUrl(), ds.getUsername(), ds.getPassword(), database));
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("显示所有表出错", e);
        }

        return tables;
    }

    /**
     * 显示所有修改时间字段
     *
     * @param id 数据源 ID
     * @param database 数据库名
     * @param table 表名
     *
     * @return 字段
     *
     * */
    public Map<String, String> getTimeColumns(Long id, String database, String table) {
        Map<String, String> columns = new LinkedHashMap<>();

        DataSource ds = this.queryDataSourceById(id);
        try {
            Map<String, String> all = JDBCUtils.describeTable(ds.getUrl(), ds.getUsername(), ds.getPassword(), database, table);
            Map<String, String> modifyTimeColumns = all.entrySet().stream()
                    .filter(entry -> entry.getValue().equalsIgnoreCase("datetime") || entry.getValue().equalsIgnoreCase("timestamp"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            columns.putAll(modifyTimeColumns);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return columns;
    }

}
