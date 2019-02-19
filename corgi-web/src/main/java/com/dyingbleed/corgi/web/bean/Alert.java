package com.dyingbleed.corgi.web.bean;

import lombok.Data;

/**
 * Created by 李震 on 2019/2/19.
 */
@Data
public class Alert {

    private Long id;

    private Integer level;

    private String type;

    private Long batchTaskId;

    private String batchTaskName;

    private String msg;

}
