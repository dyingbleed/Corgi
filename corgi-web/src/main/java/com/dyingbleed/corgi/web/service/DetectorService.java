package com.dyingbleed.corgi.web.service;

import com.dyingbleed.corgi.web.bean.Alert;

import java.util.List;

/**
 * Created by 李震 on 2019/2/19.
 */
public interface DetectorService {

    public void saveOrUpdateAlert(Alert alert);

    public void deleteAlert(String type, Long batchTaskId);

    public List<Alert> queryAllAlerts();

}
