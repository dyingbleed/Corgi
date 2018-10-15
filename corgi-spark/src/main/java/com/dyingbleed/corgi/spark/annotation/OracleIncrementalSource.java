package com.dyingbleed.corgi.spark.annotation;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by 李震 on 2018/10/10.
 */
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface OracleIncrementalSource {
}
