package com.dyingbleed.corgi.spark.measure

import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/13.
  */
case class Measure(
                    name: String,
                    submissionTime: LocalDateTime, // 提交时间
                    completionTime: LocalDateTime, // 完成时间
                    elapsedSeconds: Long, // 运行秒数
                    inputRows: Long, // 输入数据行数
                    inputData: Long, // 输入数据大小
                    outputRows: Long, // 输出数据行数
                    outputData: Long // 输出数据大小
                  )
