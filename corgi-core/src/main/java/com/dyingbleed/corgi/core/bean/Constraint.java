package com.dyingbleed.corgi.core.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by 李震 on 2019/4/1.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Constraint implements Serializable {

    public enum ConstraintType {
        PRIMARY_KEY,
        UNIQUE
    }

    private String name;

    private ConstraintType type;

    private Column[] columns;

}
