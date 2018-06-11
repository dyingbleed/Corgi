package com.dyingbleed.corgi.web.func;

import com.dyingbleed.corgi.web.utils.DistCpUtils;
import com.dyingbleed.corgi.web.utils.HDFSUtils;
import com.dyingbleed.corgi.web.utils.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Created by 李震 on 2018/6/11.
 */
class SyncTask implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    private SyncTaskParam param;

    public SyncTask(SyncTaskParam param) {
        this.param = param;
    }

    @Override
    public void run() {
        String tmpPath = "/tmp/corgi/" + param.getSinkDb() + "/" + param.getSinkTable(); // 临时目录

        /*
         * 1. 导出 Hive 表到临时目录
         * */
        try {
            if (param.isIncrement()) {
                HiveUtils.exportTablePartition(
                        param.getHiveMasterUrl(),
                        param.getHiveMasterUsername(),
                        param.getHiveMasterPassword(),
                        param.getSinkDb(),
                        param.getSinkTable(),
                        "ods_date",
                        LocalDate.now().format(DateTimeFormatter.ISO_DATE),
                        tmpPath
                );
            } else {
                HiveUtils.exportTable(param.getHiveMasterUrl(), param.getHiveMasterUsername(), param.getHiveMasterPassword(), param.getSinkDb(), param.getSinkTable(), tmpPath);
            }
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }

        /*
         * 2. 拷贝到备份集群临时目录
         * */
        try {
            DistCpUtils.distcp(param.getHdfsMasterUrl() + tmpPath, param.getHdfsSlaveUrl() + tmpPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                HDFSUtils.rmdir(param.getHdfsMasterUrl(), tmpPath); // 移除主集群临时目录
            } catch (IOException e) {
                logger.error("移除主集群临时目录 " + tmpPath + "失败", e);
            }
        }

        /*
         * 3. 备份集群导入 Hive 表
         * */
        try {
            if (param.isIncrement()) {
                HiveUtils.importTablePartition(
                        param.getHiveSlaveUrl(),
                        param.getHiveSlaveUsername(),
                        param.getHiveSlavePassword(),
                        param.getSinkDb(),
                        param.getSinkTable(),
                        "ods_date",
                        LocalDate.now().format(DateTimeFormatter.ISO_DATE),
                        tmpPath
                );
            } else {
                HiveUtils.dropTable(param.getHiveSlaveUrl(), param.getHiveSlaveUsername(), param.getHiveSlavePassword(), param.getSinkDb(), param.getSinkTable());
                HiveUtils.importTable(param.getHiveSlaveUrl(), param.getHiveSlaveUsername(), param.getHiveSlavePassword(), param.getSinkDb(), param.getSinkTable(), tmpPath);
            }
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                HDFSUtils.rmdir(param.getHdfsSlaveUrl(), tmpPath); // 移除备份集群临时目录
            } catch (IOException e) {
                logger.error("移除备份集群临时目录" + tmpPath + " 失败", e);
            }
        }
    }

}
