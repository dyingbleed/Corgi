package com.dyingbleed.corgi.dm.sink

import com.dyingbleed.corgi.core.bean.Constraint
import com.dyingbleed.corgi.dm.core.Conf
import com.google.inject.Inject

/**
  * Created by 李震 on 2019/4/1.
  */
class OracleSinkTable(@Inject() conf: Conf) extends SinkTable(conf.url, conf.username, conf.password, conf.sinkDB, conf.sinkTable) {

  override def constraints: Seq[Constraint] = {
    throw new NotImplementedError()
  }

}
