package com.dyingbleed.corgi.ods

import org.apache.livy.{Job, JobContext}

/**
  * Created by 李震 on 2019/3/20.
  */
class LivyJob(args: Array[String]) extends Job[Unit] {

  override def call(ctx: JobContext): Unit = {
    Application.execute(ctx.sparkSession(), args)
  }

}
