package com.dyingbleed.corgi.dm.aop

import com.dyingbleed.corgi.dm.core.SinkTable
import com.google.inject.Inject
import org.aopalliance.intercept.{MethodInterceptor, MethodInvocation}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by 李震 on 2019/3/20.
  */
class SinkOptimizationAOP extends MethodInterceptor {

  @Inject
  var sinkTable: SinkTable = _

  override def invoke(invocation: MethodInvocation): AnyRef = {
    val args = invocation.getArguments
    assert(args.size == 1)
    assert(args(0).isInstanceOf[DataFrame])

    val df: DataFrame = args(0).asInstanceOf[DataFrame]
    val optimizedDF = optimize(df)
    args(0) = optimizedDF

    invocation.proceed()
  }

  /**
    * 优化
    *
    * 1. 字段与 Sink 表保持一致
    * 2. 字段名与 Sink 表保持一致
    *
    * */
  protected def optimize(df: DataFrame): DataFrame = {
    val selectColumnNames = new mutable.ListBuffer[Column]
    val renameColumnNames = new mutable.ListBuffer[String]
    val columns = sinkTable.columns

    for (f <- df.schema.fields) {
      breakable {
        for (c <- columns) {
          if (f.name.equalsIgnoreCase(c.getName)) {
            selectColumnNames += col(f.name)
            renameColumnNames += c.getName
            break
          }
        }
      }
    }

    df.select(selectColumnNames:_*)
      .toDF(renameColumnNames:_*)
  }

}
