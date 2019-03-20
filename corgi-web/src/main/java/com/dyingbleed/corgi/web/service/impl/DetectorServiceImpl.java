package com.dyingbleed.corgi.web.service.impl;

import com.dyingbleed.corgi.web.bean.Alert;
import com.dyingbleed.corgi.web.mapper.AlertMapper;
import com.dyingbleed.corgi.web.service.DetectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by 李震 on 2019/2/19.
 */
@Service
public class DetectorServiceImpl implements DetectorService {

    @Autowired
    private AlertMapper alertMapper;

    @Override
    @Transactional
    public void saveOrUpdateAlert(Alert alert) {
        Alert existAlert = this.alertMapper.queryAlert(alert.getType(), alert.getBatchTaskId());
        if (existAlert == null) { // 如果没有告警，则增加
            this.alertMapper.insertAlert(alert);
        } else if (alert.getLevel() < existAlert.getLevel()) { // 如果级别更高，则更新
            this.alertMapper.updateAlert(alert);
        }
    }

    @Override
    @Transactional
    public void deleteAlert(String type, Long batchTaskId) {
        this.alertMapper.deleteAlert(type, batchTaskId);
    }

    @Override
    public List<Alert> queryAllAlerts() {
        return this.alertMapper.queryAllAlerts();
    }
}
