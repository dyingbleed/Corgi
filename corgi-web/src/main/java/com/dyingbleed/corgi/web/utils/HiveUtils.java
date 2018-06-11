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
     * 删除表
     *
     * @param url
     * @param username
     * @param password
     * @param db
     * @param table
     *
     * */
    public static void dropTable(String url, String username, String password, String db, String table) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists " + db + "." + table);
        }
    }

    /**
     * 导出 Hive 表
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     * @param path
     *
     * */
    public static void exportTable(String url, String username, String password, String database, String table, String path) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("export table " + database + "." + table + " to '" + path + "'");
        }
    }

    /**
     * 导出 Hive 表分区
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     * @param partitionColumn 分区列
     * @param partitionVal 分区值
     * @param path
     *
     * */
    public static void exportTablePartition(String url, String username, String password, String database, String table, String partitionColumn, String partitionVal, String path)  throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("export table " + database + "." + table + " partition (" + partitionColumn + "='" + partitionVal + "') to '" + path + "'");
        }
    }

    /**
     * 导入 Hive 表
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     * @param path
     *
     * */
    public static void importTable(String url, String username, String password, String database, String table, String path) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("use " + database);
            statement.execute("import from '" + path + "'");
        }
    }

    /**
     * 导入 Hive 表分区
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     * @param partitionColumn 分区列
     * @param partitionVal 分区值
     * @param path
     *
     * */
    public static void importTablePartition(String url, String username, String password, String database, String table, String partitionColumn, String partitionVal, String path)  throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("use " + database);
            statement.execute("import from '" + path + "'");
        }
    }

}
