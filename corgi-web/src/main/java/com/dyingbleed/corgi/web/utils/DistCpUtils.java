package com.dyingbleed.corgi.web.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by 李震 on 2018/6/7.
 */
public class DistCpUtils {

    public static void distcp(String masterPath, String slavePath) throws Exception {
        DistCp distCp = new DistCp();
        Configuration conf = getConf();
        ToolRunner.run(conf, distCp, new String[]{"-overwrite", masterPath, slavePath}); // 覆盖
    }

    private static Configuration getConf() {
        Configuration conf = new Configuration();

        String confDir = System.getenv("HADOOP_CONF_DIR");
        conf.addResource(new Path("file://" + confDir + "/core-site.xml"));
        conf.addResource(new Path("file://" + confDir + "/hdfs-site.xml"));
        conf.addResource(new Path("file://" + confDir + "/yarn-site.xml"));

        return conf;
    }

}
