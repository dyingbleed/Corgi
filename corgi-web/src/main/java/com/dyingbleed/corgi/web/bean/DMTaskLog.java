package com.dyingbleed.corgi.web.bean;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * Created by 李震 on 2019/3/21.
 */
@Data
public class DMTaskLog {

    private Long id;

    private Long taskId;

    private String state;

    private String content;

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date ts;

    public static DMTaskLog successLog(Long taskId) {
        DMTaskLog log = new DMTaskLog();
        log.setTaskId(taskId);
        log.setState("success");
        return log;
    }

    public static DMTaskLog failedLog(Long taskId, Exception e) {
        DMTaskLog log = new DMTaskLog();
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
