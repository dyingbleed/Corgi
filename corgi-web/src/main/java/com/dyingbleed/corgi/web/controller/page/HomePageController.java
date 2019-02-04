package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * 首页
 *
 * Created by 李震 on 2018/5/10.
 */
@Controller
@RequestMapping("/")
public class HomePageController {

    /**
     * 首页
     * */
    @GetMapping
    public ModelAndView homePage() {
        ModelAndView modelAndView = new ModelAndView("index");
        return modelAndView;
    }

}
