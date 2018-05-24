package com.dyingbleed.corgi.web.controller.page;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by 李震 on 2018/5/10.
 */
@Controller
@RequestMapping("/datasource")
public class DataSourcePageController {

    @GetMapping("/")
    public ModelAndView home() {
        ModelAndView modelAndView = new ModelAndView("/datasource/index");
        return modelAndView;
    }

    @GetMapping("/editor")
    public ModelAndView editor() {
        ModelAndView modelAndView = new ModelAndView("/datasource/editor");
        return modelAndView;
    }

    @GetMapping("/editor/{id}")
    public ModelAndView editor(@PathVariable("id") Long id) {
        ModelAndView modelAndView = new ModelAndView("/datasource/editor");

        ModelMap modelMap = modelAndView.getModelMap();
        modelMap.addAttribute("id", id);

        return modelAndView;
    }

}
