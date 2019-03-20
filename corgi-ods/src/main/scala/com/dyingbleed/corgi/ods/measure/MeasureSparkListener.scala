package com.dyingbleed.corgi.ods.measure

import com.dyingbleed.corgi.ods.bean.Measure
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

/**
  * Created by 李震 on 2018/6/13.
  */
class MeasureSparkListener extends SparkListener {

  private var inputRows: Long = 0l

  private var inputData: Long = 0l

  private var outputRows: Long = 0l

  private var outputData: Long = 0l

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val im = stageCompleted.stageInfo.taskMetrics.inputMetrics
    inputRows += im.recordsRead
    inputData += im.bytesRead

    val om = stageCompleted.stageInfo.taskMetrics.outputMetrics
    outputRows += om.recordsWritten
    outputData += om.bytesWritten
  }

  def measure: Measure = {
    Measure(null, null, null, 0l, inputRows, inputData, outputRows, outputData)
  }

}
