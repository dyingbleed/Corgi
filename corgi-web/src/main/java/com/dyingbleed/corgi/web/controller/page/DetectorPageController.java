package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * 数据监测
 *
 * Created by 李震 on 2019/2/15.
 */
@Controller
@RequestMapping("/detector")
public class DetectorPageController {

    /**
     * 首页
     * */
    @GetMapping
    public ModelAndView homePage() {
        ModelAndView modelAndView = new ModelAndView("detector/index");
        return modelAndView;
    }

}
