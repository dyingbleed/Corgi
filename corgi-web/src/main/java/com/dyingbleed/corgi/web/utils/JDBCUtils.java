package com.dyingbleed.corgi.web.utils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by 李震 on 2018/7/3.
 */
public class JDBCUtils {

    /**
     * 测试数据库连接
     *
     * @param url
     * @param username
     * @param password
     *
     * */
    public static void testConnection(String url, String username, String password) throws ClassNotFoundException, SQLException {
        if (url.startsWith("jdbc:mysql")) {
            MySQLUtils.testConnection(url, username, password);
        } else if (url.startsWith("jdbc:oracle:thin")) {
            OracleUtils.testConnection(url, username, password);
        } else {
            throw new IllegalArgumentException("不支持的数据源");
        }
    }

    /**
     * 显示所有数据库
     *
     * @param url
     * @param username
     * @param password
     *
     * @return 数据库列表
     *
     * */
    public static List<String> showDatabases(String url, String username, String password) throws ClassNotFoundException, SQLException {
        if (url.startsWith("jdbc:mysql")) {
            return MySQLUtils.showDatabases(url, username, password);
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return OracleUtils.showDatabases(url, username, password);
        } else {
            throw new IllegalArgumentException("不支持的数据源");
        }
    }

    /**
     * 显示所有表
     *
     * @param url
     * @param username
     * @param password
     * @param database
     *
     * @return 表列表
     *
     * */
    public static List<String> showTables(String url, String username, String password, String database) throws ClassNotFoundException, SQLException {
        if (url.startsWith("jdbc:mysql")) {
            return MySQLUtils.showTables(url, username, password, database);
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return OracleUtils.showTables(url, username, password, database);
        } else {
            throw new IllegalArgumentException("不支持的数据源");
        }
    }

    /**
     * 显示所有表字段
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     *
     * @return 表字段
     *
     * */
    public static Map<String, String> describeTable(String url, String username, String password, String database, String table) throws SQLException, ClassNotFoundException {
        if (url.startsWith("jdbc:mysql")) {
            return MySQLUtils.describeTable(url, username, password, database, table);
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return OracleUtils.describeTable(url, username, password, database, table);
        } else {
            throw new IllegalArgumentException("不支持的数据源");
        }
    }

}
