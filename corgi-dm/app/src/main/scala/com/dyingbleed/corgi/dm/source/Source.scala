package com.dyingbleed.corgi.dm.source

import com.dyingbleed.corgi.dm.core.Conf
import com.google.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDate

/**
  * Created by 李震 on 2019/3/12.
  */
class Source {

  @Inject
  var spark: SparkSession = _

  @Inject
  var conf: Conf = _

  def source(): DataFrame = {
    if (conf.whereExp.isDefined && conf.whereExp.get.trim.length > 0) {
      val date = if (conf.dayOffset.isDefined) {
        LocalDate.now().plusDays(conf.dayOffset.get)
      } else {
        LocalDate.now()
      }

      val filterExp = raw"date\('([^']*)'\)".r.replaceAllIn(conf.whereExp.get, m => s"'${date.toString(m.group(1))}'")

      spark.table(s"${conf.sourceDB}.${conf.sourceTable}")
        .filter(filterExp)
    } else {
      spark.table(s"${conf.sourceDB}.${conf.sourceTable}")
    }
  }

}
