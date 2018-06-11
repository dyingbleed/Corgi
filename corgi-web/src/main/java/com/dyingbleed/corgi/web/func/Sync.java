package com.dyingbleed.corgi.web.func;

import com.dyingbleed.corgi.web.bean.BatchTask;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.*;

/**
 * Created by 李震 on 2018/6/11.
 */
@Component
@PropertySource("file:${CORGI_HOME}/conf/cluster.properties")
public class Sync {

    @Value("${hdfs.master.url}")
    private String hdfsMasterUrl;

    @Value("${hdfs.slave.url}")
    private String hdfsSlaveUrl;

    @Value("${hive.master.url}")
    private String hiveMasterUrl;

    @Value("${hive.master.username}")
    private String hiveMasterUsername;

    @Value("${hive.master.password}")
    private String hiveMasterPassword;

    @Value("${hive.slave.url}")
    private String hiveSlaveUrl;

    @Value("${hive.slave.username}")
    private String hiveSlaveUsername;

    @Value("${hive.slave.password}")
    private String hiveSlavePassword;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * 增量同步批处理任务（异步）
     *
     * @param task 批处理任务
     */
    public void syncBatchTaskIncrementally(BatchTask task) {
        checking();

        SyncTaskParam param = createParam(task);
        param.setIncrement(true);
        this.threadPool.submit(new SyncTask(param));
    }

    /**
     * 全量同步批处理任务（异步）
     *
     * @param task 批处理任务
     */
    public void syncBatchTaskTotally(BatchTask task) {
        checking();

        SyncTaskParam param = createParam(task);
        param.setIncrement(false);
        this.threadPool.submit(new SyncTask(param));
    }

    private void checking() {
        // 检查配置项
        checkNotNull(this.hdfsMasterUrl);
        checkNotNull(this.hdfsSlaveUrl);
        checkNotNull(this.hiveMasterUrl);
        checkNotNull(this.hiveSlaveUrl);

        // 检查环境变量
        checkNotNull(System.getenv("HADOOP_HOME"));
        checkNotNull(System.getenv("HADOOP_CONF_DIR"));

        // 检查配置文件
        String confDir = System.getenv("HADOOP_CONF_DIR");
        checkState(new File(String.join(File.separator, confDir, "core-site.xml")).exists(), "core-site.xml 配置文件不存在");
        checkState(new File(String.join(File.separator, confDir, "hdfs-site.xml")).exists(), "hdfs-site.xml 配置文件不存在");
        checkState(new File(String.join(File.separator, confDir, "yarn-site.xml")).exists(), "yarn-site.xml 配置文件不存在");
    }

    private SyncTaskParam createParam(BatchTask task) {
        return new SyncTaskParam(
                this.hdfsMasterUrl,
                this.hdfsSlaveUrl,
                this.hiveMasterUrl,
                this.hiveMasterUsername,
                this.hiveMasterPassword,
                this.hiveSlaveUrl,
                this.hiveSlaveUsername,
                this.hiveSlavePassword,
                task.getSinkDb(),
                task.getSinkTable()
        );
    }

}
