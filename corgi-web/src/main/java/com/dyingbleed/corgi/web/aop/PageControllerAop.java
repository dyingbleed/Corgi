package com.dyingbleed.corgi.web.aop;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.ui.ModelMap;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 李震 on 2018/5/17.
 */
@Aspect
@Component
public class PageControllerAop {

    @Value("${server.display-name}")
    private String appName;

    @Pointcut("execution(org.springframework.web.servlet.ModelAndView com.dyingbleed.corgi.web.controller.page.*Controller.*(..))")
    private void method() {}

    @AfterReturning(returning = "modelAndView", pointcut = "method()")
    public ModelAndView addGlobalAttribute(ModelAndView modelAndView) {
        ModelMap modelMap = modelAndView.getModelMap();
        modelMap.addAttribute("appName", appName);
        return modelAndView;
    }

}
