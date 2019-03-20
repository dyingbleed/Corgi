package com.dyingbleed.corgi.core.constant;

/**
 * Created by 李震 on 2019/3/14.
 */
public enum DBMSVendor {

    MYSQL("com.mysql.jdbc.Driver"),
    ORACLE("oracle.jdbc.OracleDriver"),
    HIVE2("org.apache.hive.jdbc.HiveDriver");

    private String driverClassName;

    DBMSVendor(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public static DBMSVendor fromURL(String url) {
        if (url.startsWith("jdbc:mysql")) {
            return DBMSVendor.MYSQL;
        } else if(url.startsWith("jdbc:oracle:thin")) {
            return DBMSVendor.ORACLE;
        } else if (url.startsWith("jdbc:hive2")) {
            return DBMSVendor.HIVE2;
        } else {
            throw new IllegalArgumentException("Not support");
        }
    }
}
