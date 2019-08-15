package com.dyingbleed.corgi.core.util;

import com.dyingbleed.corgi.core.bean.Column;
import com.dyingbleed.corgi.core.bean.Constraint;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author 李震
 * @since 2019-08-14
 */
public class TableConstraintUtil {

    public static boolean isConstraint(List<Constraint> constraints, Set<Column> columns) {
        boolean isConstraint = false;

        for (Constraint constraint : constraints) {
            HashSet<Column> constraintColumns = new HashSet<>();
            Collections.addAll(constraintColumns, constraint.getColumns());
            if (constraintColumns.equals(columns)) {
                isConstraint = true;
                break;
            }
        }

        return isConstraint;
    }

}
