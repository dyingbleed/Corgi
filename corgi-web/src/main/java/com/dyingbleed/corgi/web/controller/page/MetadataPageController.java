package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * 元数据管理
 *
 * Created by 李震 on 2018/6/12.
 */
@Controller
@RequestMapping("/metadata")
public class MetadataPageController {

    /**
     * 首页
     * */
    @RequestMapping("/")
    public ModelAndView homePage() {
        ModelAndView modelAndView = new ModelAndView("metadata/index");
        return modelAndView;
    }

}
