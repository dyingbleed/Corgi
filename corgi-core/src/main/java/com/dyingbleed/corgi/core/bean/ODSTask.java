package com.dyingbleed.corgi.core.bean;

import lombok.Data;

/**
 * 批处理任务 POJO
 *
 * Created by 李震 on 2018/5/15.
 */
@Data
public class ODSTask {

    private Long id;

    private String name;

    private Long datasourceId;

    private String datasourceUrl;

    private String datasourceUsername;

    private String datasourcePassword;

    private String sourceDb;

    private String sourceTable;

    private String mode;

    private String timeColumn;

    private String sinkDb;

    private String sinkTable;

}
