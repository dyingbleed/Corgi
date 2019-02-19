package com.dyingbleed.corgi.web.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Created by 李震 on 2018/6/12.
 */
@Data
@EqualsAndHashCode(of = {"name", "dataType"})
public class Column {

    private String name;

    private Integer dataType;

    private String typeName;

    private String comment;

    public Column(String name, Integer dataType, String type, String comment) {
        this.name = name;
        this.dataType = dataType;
        this.typeName = type;
        this.comment = comment;
    }
}
