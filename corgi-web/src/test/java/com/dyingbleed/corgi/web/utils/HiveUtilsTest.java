package com.dyingbleed.corgi.web.utils;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

/**
 * Created by 李震 on 2018/5/17.
 */
public class HiveUtilsTest extends TestCase {

    @Test
    public void testShowDatabases() throws Exception {
        List<String> databases = HiveUtils.showDatabases();
        assertTrue(databases.size() > 0);
        for (String db:
             databases) {
            System.out.println(db);
        }
    }

    @Test
    public void testShowTables() throws Exception {
        List<String> tables = HiveUtils.showTables("dw");
        assertTrue(tables.size() > 0);
        for (String table:
                tables) {
            System.out.println(table);
        }
    }
}
