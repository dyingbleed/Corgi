package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.web.bean.Column;
import com.dyingbleed.corgi.web.service.HiveService;
import com.dyingbleed.corgi.web.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Hive Metadata
 *
 * Created by 李震 on 2018/5/17.
 */
@Service
@PropertySource("file:${CORGI_HOME}/conf/cluster.properties")
public class HiveServiceImpl implements HiveService {

    private static final Logger logger = LoggerFactory.getLogger(HiveServiceImpl.class);

    @Value("${hive.master.url}")
    private String hiveMasterUrl;

    @Value("${hive.master.username}")
    private String hiveMasterUsername;

    @Value("${hive.master.password}")
    private String hiveMasterPassword;

    @Autowired
    private BatchTaskServiceImpl batchTaskService;

    /**
     * 显示 Hive 所有数据库
     *
     * @return 数据库列表
     *
     * */
    @Override
    @Cacheable(cacheNames = "sink_db")
    public List<String> showDBs() {
        List<String> databases = new LinkedList<>();
        try {
            databases.addAll(JDBCUtils.showDatabases(this.hiveMasterUrl, this.hiveMasterUsername, this.hiveMasterPassword));
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("显示所有 Hive 数据库出错", e);
        }
        return databases;
    }

    /**
     * 显示 Hive 数据库所有表
     *
     * @param db 数据库名
     *
     * @return 表列表
     *
     * */
    @Override
    @Cacheable(cacheNames = "sink_table")
    public List<String> showTables(String db) {
        List<String> tables = new LinkedList<>();
        try {
            tables.addAll(JDBCUtils.showTables(this.hiveMasterUrl, this.hiveMasterUsername, this.hiveMasterPassword, db));
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("显示所有 Hive 数据库表出错", e);
        }
        return tables;
    }

    /**
     * 显示 Hive 表所有字段
     *
     * @param db
     * @param table
     *
     * @return 表字段
     * */
    @Override
    public List<Column> descTable(String db, String table) {
        try {
            return JDBCUtils.descTable(this.hiveMasterUrl, this.hiveMasterUsername, this.hiveMasterPassword, db, table);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("显示 Hive 表列出错", e);
            throw new RuntimeException(e);
        }
    }

}
