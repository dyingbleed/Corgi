package com.dyingbleed.corgi.core.util;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.core.constant.DBMSVendor;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 李震 on 2019/3/13.
 */
public class JDBCUtil {

    /**
     * 获取数据库连接
     *
     * @param url
     * @param username
     * @param password
     *
     * @return 数据库连接
     *
     * */
    public static Connection getConnection(String url, String username, String password) throws ClassNotFoundException, SQLException {
        assert url != null;
        assert username != null;
        assert password != null;

        Class.forName(DBMSVendor.fromURL(url).getDriverClassName());

        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 自动关闭连接
     *
     * @param url
     * @param username
     * @param password
     * @param withConnection 回调
     *
     * */
    public static void withAutoClose(String url, String username, String password, WithConnection withConnection) throws Exception {
        try (Connection conn = getConnection(url, username, password)) {
            withConnection.withConnection(conn);
        }
    }

    /**
     * 事务
     *
     * @param url
     * @param username
     * @param password
     * @param withConnection 回调
     *
     * */
    public static void withTransaction(String url, String username, String password, WithConnection withConnection) throws Exception {
        withAutoClose(url, username, password, (conn) -> {
            conn.setAutoCommit(false);
            try {
                withConnection.withConnection(conn);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        });
    }

    /**
     * 事务
     *
     * @param conn
     * @param withConnection 回调
     *
     * */
    public static void withTransaction(Connection conn, WithConnection withConnection) throws Exception {
        conn.setAutoCommit(false);
        try {
            withConnection.withConnection(conn);
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * 显示表所有字段
     *
     * @param conn
     * @param db
     * @param table
     *
     * @return 表字段
     *
     * */
    public static List<Column> getColumns(Connection conn, String db, String table) throws SQLException {
        List<Column> columns = new LinkedList<>();

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

        return columns;
    }

    /**
     * 显示表主键
     *
     * @param conn
     * @param db
     * @param table
     *
     * @return 主键
     *
     * */
    public static List<Column> getPrimaryKey(Connection conn, String db, String table) throws SQLException {
        List<Column> pks = new LinkedList<>();

        DatabaseMetaData dbmd = conn.getMetaData();
        try (ResultSet pkrs = dbmd.getPrimaryKeys(db, null, table)) {
            while (pkrs.next()) {
                String pkName = pkrs.getString(4);
                try (ResultSet rs = dbmd.getColumns(conn.getCatalog(), db, table, pkName)) {
                    while (rs.next()) {
                        String columnName = rs.getString(4);
                        int dataType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        String comment = rs.getString(12);
                        pks.add(new Column(columnName, dataType, typeName, comment));
                    }
                }
            }
        }

        return pks;
    }

    /**
     * 获取表基数
     *
     * @param conn
     * @param db
     * @param table
     *
     * @return 表字段
     *
     * */
    public static Long getCardinality(Connection conn, String db, String table) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT count(1) FROM " + db + "." + table)) {
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    return 0L;
                }
            }
        }
    }

    @FunctionalInterface
    public interface WithConnection {

        public void withConnection(Connection conn) throws Exception;

    }

}
