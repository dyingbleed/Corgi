package com.dyingbleed.corgi.web.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 李震 on 2019/3/11.
 */
@Controller
@RequestMapping("/dm")
public class DMTaskPageController {

    /**
     * 首页
     * */
    @GetMapping("/")
    public ModelAndView home() {
        ModelAndView modelAndView = new ModelAndView("dm/index");
        return modelAndView;
    }

    /**
     * 新建批处理任务
     * */
    @GetMapping("/editor")
    public ModelAndView editor() {
        ModelAndView modelAndView = new ModelAndView("dm/editor");
        return modelAndView;
    }

    /**
     * 编辑批处理任务
     * */
    @GetMapping("/editor/{id}")
    public ModelAndView editor(@PathVariable("id") Long id) {
        ModelAndView modelAndView = new ModelAndView("dm/editor");

        ModelMap modelMap = modelAndView.getModelMap();
        modelMap.addAttribute("id", id);

        return modelAndView;
    }

}
