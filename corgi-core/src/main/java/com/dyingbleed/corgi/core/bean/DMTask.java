package com.dyingbleed.corgi.core.bean;

import lombok.Data;

/**
 * Created by 李震 on 2019/3/12.
 */
@Data
public class DMTask {

    private Long id;

    private String name;

    private String sourceDB;

    private String sourceTable;

    private String whereExp;

    private Integer dayOffset;

    private Long datasourceId;

    private String datasourceUrl;

    private String datasourceUsername;

    private String datasourcePassword;

    private String sinkDB;

    private String sinkTable;

    private String mode;

    private String pks;

}
