package com.dyingbleed.corgi.spark.core

import javax.inject.Named

import com.google.inject.Inject
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import redis.clients.jedis.Jedis

/**
  * Created by 李震 on 2018/3/1.
  */
class Metadata @Inject()(@Named("appName") appName: String){

  @Inject
  var conf: Conf = _

  def getLastModifyDate: LocalDateTime = {
    val jedis = new Jedis(conf.cacheHost, conf.cachePort)
    jedis.auth(conf.cachePassword)
    val key = s"spark_ods_job_${appName}"

    val lastModifyTime = if (jedis.exists(key)) {
      // 缓存命中
      LocalDateTime.parse(jedis.get(key), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    } else {
      // 缓存未命中，取前天零点
      LocalDateTime.now().minusDays(1).withTime(0, 0, 0, 0)
    }

    jedis.close()

    lastModifyTime
  }

  def saveLastModifyDate(lastModifyTime: LocalDateTime): Unit = {
    val jedis = new Jedis(conf.cacheHost, conf.cachePort)
    jedis.auth(conf.cachePassword)
    val key = s"spark_ods_job_${appName}"
    jedis.set(key, lastModifyTime.toString("yyyy-MM-dd HH:mm:ss"))
    jedis.close()
  }

}
