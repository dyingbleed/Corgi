package com.dyingbleed.corgi.web.aop;

import org.apache.shiro.SecurityUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.ui.ModelMap;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 李震 on 2018/5/17.
 */
@Aspect
@Component
public class PageControllerAOP {

    private static final Logger logger = LoggerFactory.getLogger(PageControllerAOP.class);

    @Value("${server.display-name}")
    private String appName;

    @Before("execution(org.springframework.web.servlet.ModelAndView com.dyingbleed.corgi.web.controller.page.*Controller.*(..))")
    private void log(JoinPoint joinPoint) {
        if (SecurityUtils.getSubject().isAuthenticated()) {
            String principal = (String) SecurityUtils.getSubject().getPrincipal();
            String requestURI = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getRequestURI();
            logger.info(principal + " visit page " + requestURI);
        }
    }

    @Pointcut("execution(org.springframework.web.servlet.ModelAndView com.dyingbleed.corgi.web.controller.page.*Controller.*(..))")
    private void method() {}

    @AfterReturning(returning = "modelAndView", pointcut = "method()")
    public ModelAndView addGlobalAttribute(ModelAndView modelAndView) {
        ModelMap modelMap = modelAndView.getModelMap();
        modelMap.addAttribute("appName", appName);
        return modelAndView;
    }

}
