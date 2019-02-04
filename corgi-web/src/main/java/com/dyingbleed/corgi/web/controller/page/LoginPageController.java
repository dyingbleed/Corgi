package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * 登录页
 *
 * Created by 李震 on 2019/2/2.
 */
@Controller
@RequestMapping("/login")
public class LoginPageController {

    /**
     * 登录页
     * */
    @GetMapping
    public ModelAndView loginPage() {
        ModelAndView modelAndView = new ModelAndView("login");
        return modelAndView;
    }

}
