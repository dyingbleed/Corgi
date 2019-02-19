package com.dyingbleed.corgi.web.bean;

import lombok.Data;

/**
 * 数据源 POJO
 *
 * Created by 李震 on 2018/5/9.
 */
@Data
public class DataSource {

    private Long id;

    private String name;

    private String url;

    private String username;

    private String password;

}
