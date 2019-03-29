package com.dyingbleed.corgi.web.aop;

import org.apache.shiro.SecurityUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by 李震 on 2019/3/25.
 */
@Aspect
@Component
public class ApiControllerAOP {

    private static final Logger logger = LoggerFactory.getLogger(ApiControllerAOP.class);

    @Before("execution(* com.dyingbleed.corgi.web.controller.api.*.*Controller.insert*(..)) || " +
            "execution(* com.dyingbleed.corgi.web.controller.api.*.*Controller.update*(..)) || " +
            "execution(* com.dyingbleed.corgi.web.controller.api.*.*Controller.delete*(..))")
    private void modify(JoinPoint joinPoint) {
        if (SecurityUtils.getSubject().isAuthenticated()) {
            String principal = (String) SecurityUtils.getSubject().getPrincipal();
            logger.warn(principal + " call api " + joinPoint.getSignature().toLongString());
        }
    }

}
