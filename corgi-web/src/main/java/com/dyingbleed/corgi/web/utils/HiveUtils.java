package com.dyingbleed.corgi.web.utils;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Hive Metastore 工具类
 *
 * Created by 李震 on 2018/5/17.
 */
public class HiveUtils {

    /**
     * 显示所有数据库
     *
     * @return 数据库列表
     *
     * */
    public static List<String> showDatabases(String url) throws ClassNotFoundException, SQLException {
        List<String> databases = new LinkedList<>();

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        try (Connection connection = DriverManager.getConnection(url); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("show databases")) {
            while (rs.next()) databases.add(rs.getString("database_name"));
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
    public static List<String> showTables(String url, String db) throws ClassNotFoundException, SQLException {
        List<String> tables = new LinkedList<>();

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("show tables in " + db)) {
            while (rs.next()) tables.add(rs.getString("table_name"));
        }

        return tables;
    }

}
