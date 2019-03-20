package com.dyingbleed.corgi.core.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Types;

/**
 * Created by 李震 on 2019/3/13.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"name", "type"})
public class Column implements Serializable {

    private String name;

    private Integer type;

    private String typeName;

    private String comment;

    public Boolean isNumber() {
        return this.type == Types.INTEGER ||
                this.type == Types.TINYINT ||
                this.type == Types.SMALLINT ||
                this.type ==  Types.BIGINT ||
                this.type ==  Types.FLOAT ||
                this.type ==  Types.DOUBLE ||
                this.type ==  Types.DECIMAL;
    }

    public Boolean isString() {
        return this.type == Types.VARCHAR ||
                this.type == Types.NVARCHAR ||
                this.type == Types.CHAR ||
                this.type == Types.NCHAR;
    }

    public Boolean isDate() {
        return this.type == Types.DATE;
    }

}
