package com.dyingbleed.corgi.spark.core;

/**
 * Created by 李震 on 2018/12/29.
 */
public interface Constants {

    /*
     * 参数配置
     * */
    String CONF_IGNORE_HISTORY_SHORT = "i";
    String CONF_IGNORE_HISTORY = "ignore-history";

    String CONF_PARTITION_COLUMNS_SHORT = "p";
    String CONF_PARTITION_COLUMNS = "partition-columns";

    String CONF_EXECUTE_TIME_SHORT = "t";
    String CONF_EXECUTE_TIME = "execute-time";

    /*
     * 公共变量
     * */
    String DATE_PARTITION = "ods_date";

    String DATE_FORMAT = "yyyy-MM-dd";
    String TIME_FORMAT = "HH:mm:ss";
    String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

}
