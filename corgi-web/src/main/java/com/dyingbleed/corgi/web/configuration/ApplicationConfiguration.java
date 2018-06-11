package com.dyingbleed.corgi.web.configuration;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Created by 李震 on 2018/4/12.
 */
@Configuration
@EnableCaching
@ComponentScan(basePackages = {
        "com.dyingbleed.corgi.web.controller",
        "com.dyingbleed.corgi.web.service",
        "com.dyingbleed.corgi.web.aop",
        "com.dyingbleed.corgi.web.func"
})
@Import(DBConfiguration.class)
@MapperScan(basePackages = {
        "com.dyingbleed.corgi.web.mapper"
})
public class ApplicationConfiguration {}
