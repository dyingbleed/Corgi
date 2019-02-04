package com.dyingbleed.corgi.web.controller.page;

import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * 批处理任务页
 *
 * Created by 李震 on 2018/5/15.
 */
@Controller
@RequestMapping("/batch")
public class BatchTaskPageController {

    /**
     * 首页
     * */
    @GetMapping("/")
    public ModelAndView home() {
        ModelAndView modelAndView = new ModelAndView("batch/index");
        return modelAndView;
    }

    /**
     * 新建批处理任务
     * */
    @GetMapping("/editor")
    public ModelAndView editor() {
        ModelAndView modelAndView = new ModelAndView("batch/editor");
        return modelAndView;
    }

    /**
     * 编辑批处理任务
     * */
    @GetMapping("/editor/{id}")
    @RequiresAuthentication
    public ModelAndView editor(@PathVariable("id") Long id) {
        ModelAndView modelAndView = new ModelAndView("batch/editor");

        ModelMap modelMap = modelAndView.getModelMap();
        modelMap.addAttribute("id", id);

        return modelAndView;
    }

}
