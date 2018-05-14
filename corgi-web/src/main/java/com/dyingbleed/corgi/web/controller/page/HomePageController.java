package com.dyingbleed.corgi.web.controller.page;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by 李震 on 2018/5/10.
 */
@Controller
public class HomePageController {

    @Value("${server.display-name}")
    private String appName;

    @RequestMapping("/")
    public String homePage(Model model) {

        model.addAttribute("appName", this.appName);

        return "index";
    }

}
