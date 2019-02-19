package com.dyingbleed.corgi.web.controller.api;

import com.dyingbleed.corgi.web.bean.Alert;
import com.dyingbleed.corgi.web.service.DetectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by 李震 on 2019/2/19.
 */
@RestController
@RequestMapping("/api/detector")
public class DetectorController {

    @Autowired
    private DetectorService detectorService;

    @GetMapping
    public List<Alert> queryAllAlerts() {
        return this.detectorService.queryAllAlerts();
    }

}
