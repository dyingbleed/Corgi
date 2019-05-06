package com.dyingbleed.corgi.web.bean;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * Created by 李震 on 2019-05-06.
 */
@Data
public class ODSTaskLog {

    private Long id;

    private Long taskId;

    private String state;

    private String content;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date ts;

    public static ODSTaskLog successLog(Long taskId) {
        ODSTaskLog log = new ODSTaskLog();
        log.setTaskId(taskId);
        log.setState("success");
        return log;
    }

    public static ODSTaskLog failedLog(Long taskId, Exception e) {
        ODSTaskLog log = new ODSTaskLog();
        log.setTaskId(taskId);
        log.setState("failed");
        log.setContent(getStackTrace(e).substring(0, 4096));
        return log;
    }

    private static String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

}
