package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 李震 on 2018/5/10.
 */
@Controller
public class HomePageController {

    @RequestMapping("/")
    public ModelAndView homePage(Model model) {
        ModelAndView modelAndView = new ModelAndView("index");
        return modelAndView;
    }

}
