package com.dyingbleed.corgi.web.bean;

/**
 * Created by 李震 on 2018/6/14.
 */
public class MeasureStat {

    private Long taskCount;

    private Long elapsedSecondSum;

    public Long getTaskCount() {
        return taskCount;
    }

    public void setTaskCount(Long taskCount) {
        this.taskCount = taskCount;
    }

    public Long getElapsedSecondSum() {
        return elapsedSecondSum;
    }

    public void setElapsedSecondSum(Long elapsedSecondSum) {
        this.elapsedSecondSum = elapsedSecondSum;
    }
}
