package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.web.bean.Datasource;
import com.dyingbleed.corgi.web.mapper.DMTaskMapper;
import com.dyingbleed.corgi.web.mapper.DatasourceMapper;
import com.dyingbleed.corgi.web.mapper.ODSTaskMapper;
import com.dyingbleed.corgi.web.service.DatasourceService;
import com.dyingbleed.corgi.web.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 数据源
 *
 * Created by 李震 on 2018/5/9.
 */
@Service
public class DataSourceServiceImpl implements DatasourceService {

    private static Logger logger = LoggerFactory.getLogger(DataSourceServiceImpl.class);

    @Autowired
    private DatasourceMapper datasourceMapper;

    @Autowired
    private ODSTaskMapper odsTaskMapper;

    @Autowired
    private DMTaskMapper dmTaskMapper;

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    @Override
    @Transactional
    public void deleteDatasourceById(Long id) {
        this.datasourceMapper.deleteDatasourceById(id); // 删除 datasource
        this.odsTaskMapper.deleteODSTaskByDatasourceId(id); // 删除 datasource 关联的 ods_task
        this.dmTaskMapper.deleteDMTaskByDatasourceId(id); // 删除 datasource 关联的 dm_task
    }

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    @Override
    @Transactional
    public void insertOrUpdateDatasource(Datasource ds) {
        if (ds.getId() == null) {
            this.datasourceMapper.insertDatasource(ds);
        } else {
            this.datasourceMapper.updateDatasource(ds);
        }
    }

    /**
     * 查询所有数据源
     *
     * @return 数据源
     *
     * */
    @Override
    public List<Datasource> queryAllDatasource() {
        return this.datasourceMapper.queryAllDatasource();
    }

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     *
     * */
    @Override
    public Datasource queryDatasourceById(Long id) {
        return this.datasourceMapper.queryDatasourceById(id);
    }

    /**
     * 根据名称查询数据源
     *
     * @param name 数据源名称
     *
     * @return 数据源
     *
     * */
    @Override
    public Datasource queryDatasourceByName(String name) {
        return this.datasourceMapper.queryDatasourceByName(name);
    }

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 结果
     *
     * */
    @Override
    public String testConnection(Datasource ds) {
        String message = "success";
        try {
            JDBCUtils.testConnection(ds.getUrl(), ds.getUsername(), ds.getPassword());
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
    @Override
    @Cacheable(cacheNames = "source_db")
    public List<String> showDBs(Long id) {
        LinkedList<String> databases = new LinkedList<>();

        Datasource ds = this.queryDatasourceById(id);
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
    @Override
    @Cacheable(cacheNames = "source_table")
    public List<String> showTables(Long id, String database) {
        LinkedList<String> tables = new LinkedList<>();

        Datasource ds = this.queryDatasourceById(id);
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
    @Override
    public Map<String, String> getTimeColumns(Long id, String database, String table) {
        Map<String, String> timeColumnMap = new LinkedHashMap<>();

        Datasource ds = this.queryDatasourceById(id);
        try {
            List<Column> columns = JDBCUtils.descTable(ds.getUrl(), ds.getUsername(), ds.getPassword(), database, table);
            for (Column c: columns) {
                if (c.getType() == Types.DATE ||
                        c.getType() == Types.TIME ||
                        c.getType() == Types.TIMESTAMP) {
                    timeColumnMap.put(c.getName(), c.getTypeName());
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("获取所有修改时间字段失败", e);
        }

        return timeColumnMap;
    }

    @Override
    public List<Column> descTable(Long id, String database, String table) {
        Datasource ds = this.queryDatasourceById(id);
        try {
            return JDBCUtils.descTable(ds.getUrl(), ds.getUsername(), ds.getPassword(), database, table);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("获取所有字段失败", e);
            throw new RuntimeException(e);
        }
    }
}
