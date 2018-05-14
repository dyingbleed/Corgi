package com.dyingbleed.corgi.web.utils;

import java.sql.*;

/**
 * Created by 李震 on 2018/5/14.
 */
public class JDBCUtils {

    /**
     * 测试 MySQL 数据库连接
     *
     * @param url
     * @param username
     * @param password
     *
     * */
    public static void testMySQLConnection(String url, String username, String password) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement statement = connection.createStatement()) {
            statement.execute("select 1");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
