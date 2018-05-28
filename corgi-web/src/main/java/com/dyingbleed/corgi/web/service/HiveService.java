package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.utils.HiveUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

/**
 * Hive Metadata
 *
 * Created by 李震 on 2018/5/17.
 */
@Service
public class HiveService {

    private static final Logger logger = LoggerFactory.getLogger(HiveService.class);

    /**
     * 显示 Hive 所有数据库
     *
     * @return 数据库列表
     *
     * */
    @Cacheable(cacheNames = "sink_db")
    public List<String> showDBs() {
        List<String> databases = new LinkedList<>();
        try {
            databases.addAll(HiveUtils.showDatabases());
        } catch (MetaException e) {
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
    @Cacheable(cacheNames = "sink_table")
    public List<String> showTables(String db) {
        List<String> tables = new LinkedList<>();
        try {
            tables.addAll(HiveUtils.showTables(db));
        } catch (MetaException e) {
            logger.error("显示所有 Hive 数据库表出错", e);
        }
        return tables;
    }

}
