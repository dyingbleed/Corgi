package com.dyingbleed.corgi.web;

import com.dyingbleed.corgi.web.configuration.ApplicationConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Created by 李震 on 2018/4/12.
 */
@SpringBootApplication
@Import(ApplicationConfiguration.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
