package com.dyingbleed.corgi.web.utils;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.core.constant.DBMSVendor;
import com.dyingbleed.corgi.core.util.JDBCUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 李震 on 2018/7/3.
 */
public class JDBCUtils {

    private static ResultSet getDatabases(String url, DatabaseMetaData dbmd) throws SQLException {
        switch (DBMSVendor.fromURL(url)) {
            case MYSQL: return dbmd.getCatalogs();
            case ORACLE: return dbmd.getSchemas();
            case HIVE2: return dbmd.getSchemas();
            default: throw new RuntimeException("不支持的数据源");
        }
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
        try (Connection conn = JDBCUtil.getConnection(url, username, password)) {
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

        try (Connection conn = JDBCUtil.getConnection(url, username, password)) {
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

        try (Connection conn = JDBCUtil.getConnection(url, username, password)) {
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
        List<Column> r = new LinkedList<>();

        try (Connection conn = JDBCUtil.getConnection(url, username, password)) {
            r.addAll(JDBCUtil.getColumns(conn, db, table));
        }

        return r;
    }

}
