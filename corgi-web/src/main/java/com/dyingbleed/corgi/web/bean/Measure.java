package com.dyingbleed.corgi.web.bean;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

/**
 * Created by 李震 on 2018/6/13.
 */
public class Measure {

    private Long id;

    private String name;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date submissionTime;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date completionTime;

    private Long elapsedSeconds;

    private Long inputRows;

    private Long inputData;

    private Long outputRows;

    private Long outputData;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    public Date getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(Date completionTime) {
        this.completionTime = completionTime;
    }

    public Long getElapsedSeconds() {
        return elapsedSeconds;
    }

    public void setElapsedSeconds(Long elapsedSeconds) {
        this.elapsedSeconds = elapsedSeconds;
    }

    public Long getInputRows() {
        return inputRows;
    }

    public void setInputRows(Long inputRows) {
        this.inputRows = inputRows;
    }

    public Long getInputData() {
        return inputData;
    }

    public void setInputData(Long inputData) {
        this.inputData = inputData;
    }

    public Long getOutputRows() {
        return outputRows;
    }

    public void setOutputRows(Long outputRows) {
        this.outputRows = outputRows;
    }

    public Long getOutputData() {
        return outputData;
    }

    public void setOutputData(Long outputData) {
        this.outputData = outputData;
    }
}
