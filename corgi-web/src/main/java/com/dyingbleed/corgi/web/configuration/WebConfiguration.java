package com.dyingbleed.corgi.web.configuration;

import org.apache.shiro.web.env.EnvironmentLoaderListener;
import org.apache.shiro.web.servlet.ShiroFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by 李震 on 2019/2/2.
 */
@Configuration
public class WebConfiguration {

    @Bean
    public ServletListenerRegistrationBean servletListenerRegistrationBean() {
        ServletListenerRegistrationBean bean = new ServletListenerRegistrationBean();

        EnvironmentLoaderListener listener = new EnvironmentLoaderListener();
        bean.setListener(listener);

        return bean;
    }

    @Bean
    public FilterRegistrationBean filterRegistrationBean() {
        FilterRegistrationBean bean = new FilterRegistrationBean();

        ShiroFilter shiroFilter = new ShiroFilter();
        bean.setFilter(shiroFilter);
        bean.addUrlPatterns("/*");

        return bean;
    }

    @Bean
    public ServletContextInitializer servletContextInitializer() {
        return servletContext -> servletContext.setInitParameter("shiroConfigLocations", "file:" + System.getenv("CORGI_HOME") + "/conf/shiro.ini");
    }

}
