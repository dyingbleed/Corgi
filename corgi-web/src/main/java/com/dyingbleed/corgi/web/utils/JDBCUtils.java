package com.dyingbleed.corgi.web.utils;

import com.dyingbleed.corgi.web.bean.Column;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 李震 on 2018/7/3.
 */
public class JDBCUtils {

    private static String getVendor(String url) {
        if (url.startsWith("jdbc:mysql")) {
            return "mysql";
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return "oracle";
        } else if (url.startsWith("jdbc:hive2")) {
            return "hive2";
        } else {
            throw new RuntimeException("不支持的数据源");
        }
    }

    private static ResultSet getDatabases(String url, DatabaseMetaData dbmd) throws SQLException {
        switch (getVendor(url)) {
            case "mysql": return dbmd.getCatalogs();
            case "oracle": return dbmd.getSchemas();
            case "hive2": return dbmd.getSchemas();
            default: throw new RuntimeException("不支持的数据源");
        }
    }

    /**
     * 获取数据库连接
     *
     * @param url
     * @param username
     * @param password
     */
    public static Connection getConnection(String url, String username, String password) throws ClassNotFoundException, SQLException {
        switch (getVendor(url)) {
            case "mysql": Class.forName("com.mysql.jdbc.Driver"); break;
            case "oracle": Class.forName("oracle.jdbc.OracleDriver"); break;
            case "hive2": Class.forName("org.apache.hive.jdbc.HiveDriver"); break;
            default: throw new RuntimeException("不支持的数据源");
        }
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 测试数据库连接
     *
     * @param url
     * @param username
     * @param password
     *
     * */
    public static void testConnection(String url, String username, String password) throws ClassNotFoundException, SQLException {
        try (Connection conn = getConnection(url, username, password)) {
            if (conn.isClosed()) throw new RuntimeException("数据库连接失败！");
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
        List<String> schemas = new LinkedList<>();

        try (Connection conn = getConnection(url, username, password)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = getDatabases(url, dbmd)) {
                while (rs.next()) {
                    String schemaName = rs.getString(1);
                    schemas.add(schemaName);
                }
            }
        }

        return schemas;
    }

    /**
     * 显示所有表
     *
     * @param url
     * @param username
     * @param password
     * @param db
     *
     * @return 表列表
     *
     * */
    public static List<String> showTables(String url, String username, String password, String db) throws ClassNotFoundException, SQLException {
        List<String> tables = new LinkedList<>();

        try (Connection conn = getConnection(url, username, password)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTables(conn.getCatalog(), db, null, null)) {
                while (rs.next()) {
                    String tableName = rs.getString(3);
                    tables.add(tableName);
                }
            }
        }

        return tables;
    }

    /**
     * 显示所有表字段
     *
     * @param url
     * @param username
     * @param password
     * @param db
     * @param table
     *
     * @return 表字段
     *
     * */
    public static List<Column> descTable(String url, String username, String password, String db, String table) throws SQLException, ClassNotFoundException {
        List<Column> columns = new LinkedList<>();

        try (Connection conn = getConnection(url, username, password)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getColumns(conn.getCatalog(), db, table, null)) {
                while (rs.next()) {
                    String columnName = rs.getString(4);
                    int dataType = rs.getInt(5);
                    String typeName = rs.getString(6);
                    String comment = rs.getString(12);
                    columns.add(new Column(columnName, dataType, typeName, comment));
                }
            }
        }

        return columns;
    }

}
