package com.dyingbleed.corgi.web.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by 李震 on 2018/5/17.
 */
public class HiveUtils {

    /**
     * 显示所有数据库
     *
     * @return 数据库列表
     *
     * */
    public static List<String> showDatabases() throws MetaException {
        List<String> databases = new LinkedList<>();

        HiveConf conf = new HiveConf();
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(conf);
            databases.addAll(client.getAllDatabases());
        } catch (MetaException e) {
            throw e;
        } finally {
            if (client != null) client.close();
        }

        return databases;
    }

    /**
     * 显示所有表
     *
     * @param db
     *
     * @return 数据库列表
     *
     * */
    public static List<String> showTables(String db) throws MetaException {
        List<String> tables = new LinkedList<>();

        HiveConf conf = new HiveConf();
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(conf);
            tables.addAll(client.getTables(db, "*"));
        } catch (MetaException e) {
            throw e;
        } finally {
            if (client != null) client.close();
        }

        return tables;
    }

}
