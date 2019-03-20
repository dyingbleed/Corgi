package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.web.bean.Datasource;

import java.util.List;
import java.util.Map;

/**
 * Created by 李震 on 2019/2/2.
 */
public interface DatasourceService {

    /**
     * 根据 ID 删除数据源
     *
     * @param id 数据源 ID
     *
     * */
    public void deleteDatasourceById(Long id);

    /**
     * 修改数据源
     *
     * @param ds 数据源
     *
     * @return 数据源
     *
     * */
    public void insertOrUpdateDatasource(Datasource ds);

    /**
     * 查询所有数据源
     *
     * @return 数据源
     *
     * */
    public List<Datasource> queryAllDatasource();

    /**
     * 根据 ID 查询数据源
     *
     * @param id 数据源 ID
     *
     * @return 数据源
     *
     * */
    public Datasource queryDatasourceById(Long id);

    /**
     * 根据名称查询数据源
     *
     * @param name 数据源名称
     *
     * @return 数据源
     *
     * */
    public Datasource queryDatasourceByName(String name);

    /**
     * 测试数据源连接
     *
     * @param ds 数据源
     *
     * @return 结果
     *
     * */
    public String testConnection(Datasource ds);

    /**
     * 显示所有数据库
     *
     * @param id 数据源 ID
     *
     * @return 数据库列表
     *
     * */
    public List<String> showDBs(Long id);

    /**
     * 显示数据库所有表
     *
     * @param id 数据源 ID
     * @param database 数据库名
     *
     * @return 表列表
     *
     * */
    public List<String> showTables(Long id, String database);

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
    public Map<String, String> getTimeColumns(Long id, String database, String table);

    /**
     * 显示所有字段
     *
     * @param id 数据源 ID
     * @param database 数据库名
     * @param table 表名
     *
     * @return 字段
     *
     * */
    public List<Column> descTable(Long id, String database, String table);

}
