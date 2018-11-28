package com.dyingbleed.corgi.web.utils;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Hive Metastore 工具类
 *
 * Created by 李震 on 2018/5/17.
 */
public class HiveUtils {

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
        List<String> databases = new LinkedList<>();

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("show databases")) {
            while (rs.next()) databases.add(rs.getString("database_name"));
        }

        return databases;
    }

    /**
     * 显示所有表
     *
     * @param url
     * @param username
     * @param password
     * @param db
     *
     * @return 数据库列表
     *
     * */
    public static List<String> showTables(String url, String username, String password, String db) throws ClassNotFoundException, SQLException {
        List<String> tables = new LinkedList<>();

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("show tables in " + db)) {
            while (rs.next()) tables.add(rs.getString("table_name"));
        }

        return tables;
    }

    /**
     * 显示所有表所有列
     *
     * @param url
     * @param username
     * @param password
     * @param db
     * @param table
     *
     * @return 数据库列表
     *
     * */
    public static List<Map<String, String>> descTable(String url, String username, String password, String db, String table) throws ClassNotFoundException, SQLException {
        List<Map<String, String>> columns = new LinkedList<>();

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("desc " + db + "." + table)) {
            while (rs.next()) {
                Map<String, String> column = new HashMap<>();
                column.put("name", rs.getString("col_name"));
                column.put("type", rs.getString("data_type"));
                column.put("comment", rs.getString("comment"));
                columns.add(column);
            }
        }

        return columns;
    }

}
