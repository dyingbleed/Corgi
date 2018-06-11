package com.dyingbleed.corgi.web.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by 李震 on 2018/6/7.
 */
public class HDFSUtils {

    /**
     * 新建目录
     *
     * @param path
     *
     * */
    public static void mkdir(String url, String path) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path dirPath = new Path(path);
        fs.mkdirs(dirPath);
    }

    /**
     * 移除目录
     *
     * @param path
     *
     * */
    public static void rmdir(String url, String path) throws IOException  {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path dirPath = new Path(path);
        if (fs.exists(dirPath)) fs.delete(dirPath, true);
    }

    private static Configuration getConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        String confDir = System.getenv("HADOOP_CONF_DIR");
        conf.addResource(new Path("file://" + confDir + "/core-site.xml"));
        conf.addResource(new Path("file://" + confDir + "/hdfs-site.xml"));

        return conf;
    }

}
