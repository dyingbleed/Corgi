package com.dyingbleed.corgi.dm;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

/**
 * Created by 李震 on 2019/3/21.
 */
public class LivyJob implements Job<Void> {

    private String[] args;

    public LivyJob(String[] args) {
        this.args = args;
    }

    @Override
    public Void call(JobContext ctx) throws Exception {
        Application.execute(ctx.sparkSession(), this.args);
        return null;
    }

}
