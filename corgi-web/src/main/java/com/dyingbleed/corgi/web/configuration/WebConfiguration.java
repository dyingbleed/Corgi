package com.dyingbleed.corgi.web.configuration;

import org.apache.shiro.web.env.EnvironmentLoaderListener;
import org.apache.shiro.web.servlet.ShiroFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Created by 李震 on 2019/2/2.
 */
@Configuration
public class WebConfiguration {

    @Bean
    @Profile("prod")
    public ServletListenerRegistrationBean servletListenerRegistrationBean() {
        ServletListenerRegistrationBean bean = new ServletListenerRegistrationBean();

        EnvironmentLoaderListener listener = new EnvironmentLoaderListener();
        bean.setListener(listener);

        return bean;
    }

    @Bean
    @Profile("prod")
    public FilterRegistrationBean filterRegistrationBean() {
        FilterRegistrationBean bean = new FilterRegistrationBean();

        // Shiro
        ShiroFilter shiroFilter = new ShiroFilter();
        bean.setFilter(shiroFilter);
        bean.addUrlPatterns("/*");

        return bean;
    }

    @Bean
    @Profile("prod")
    public ServletContextInitializer servletContextInitializer() {
        return servletContext -> servletContext.setInitParameter("shiroConfigLocations", "file:" + System.getenv("CORGI_HOME") + "/conf/shiro.ini");
    }

}
