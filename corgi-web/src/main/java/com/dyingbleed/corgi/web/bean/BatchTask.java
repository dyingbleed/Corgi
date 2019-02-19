package com.dyingbleed.corgi.web.bean;

import lombok.Data;

/**
 * 批处理任务 POJO
 *
 * Created by 李震 on 2018/5/15.
 */
@Data
public class BatchTask {

    private Long id;

    private String name;

    private Long dataSourceId;

    private String dataSourceUrl;

    private String dataSourceUsername;

    private String dataSourcePassword;

    private String sourceDb;

    private String sourceTable;

    private String mode;

    private String timeColumn;

    private String sinkDb;

    private String sinkTable;

}
