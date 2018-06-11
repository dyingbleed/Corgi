package com.dyingbleed.corgi.web.utils;

import org.junit.Test;

/**
 * Created by 李震 on 2018/6/7.
 */
public class DistCpUtilsTest {

    @Test
    public void testDistcp() {
        try {
            DistCpUtils.distcp("hdfs://hnode1:8020/tmp/lz/webank.jar", "hdfs://localnode2:8020/tmp/lz/webank.jar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
