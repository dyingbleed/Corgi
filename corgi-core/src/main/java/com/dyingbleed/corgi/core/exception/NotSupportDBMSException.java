package com.dyingbleed.corgi.core.exception;

/**
 * Created by 李震 on 2019/4/1.
 */
public class NotSupportDBMSException extends RuntimeException {

    public NotSupportDBMSException() {
        super("DBMS is not supported!");
    }
}
