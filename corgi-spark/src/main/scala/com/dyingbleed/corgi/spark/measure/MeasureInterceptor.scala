package com.dyingbleed.corgi.spark.measure

import com.dyingbleed.corgi.spark.core.Rpc
import com.google.inject.Inject
import com.google.inject.name.Named
import org.aopalliance.intercept.{MethodInterceptor, MethodInvocation}
import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

/**
  * 启动度量
  *
  * Created by 李震 on 2018/6/13.
  */
class MeasureInterceptor extends MethodInterceptor {

  @Inject
  @Named("appName")
  private var appName: String = _

  @Inject
  private var spark: SparkSession = _

  @Inject
  private var rpc: Rpc = _

  override def invoke(invocation: MethodInvocation): AnyRef = {
    val listener = new MeasureSparkListener

    spark.sparkContext.addSparkListener(listener)
    val submissionTime = LocalDateTime.now()
    val startTime = System.nanoTime() // 开始时间
    val ret = invocation.proceed()
    val endTime = System.nanoTime() // 结束时间
    val completionTime = LocalDateTime.now()
    // spark.sparkContext.removeSparkListener(listener) // 2.2.0 之前版本不支持

    val measure = listener.measure
    rpc.saveMeasure(measure.copy(name = appName, submissionTime = submissionTime, completionTime = completionTime, elapsedSeconds = (endTime - startTime) / 1000000000l)) // 保存度量数据

    ret
  }
}