package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.core.bean.Column;

import java.util.List;

/**
 * Created by 李震 on 2019/2/2.
 */
public interface HiveService {

    /**
     * 显示 Hive 所有数据库
     *
     * @return 数据库列表
     *
     * */
    public List<String> showDBs();

    /**
     * 显示 Hive 数据库所有表
     *
     * @param db 数据库名
     *
     * @return 表列表
     *
     * */
    public List<String> showTables(String db);

    /**
     * 显示 Hive 表所有字段
     *
     * @param db
     * @param table
     *
     * @return 表字段
     * */
    public List<Column> descTable(String db, String table);

}
