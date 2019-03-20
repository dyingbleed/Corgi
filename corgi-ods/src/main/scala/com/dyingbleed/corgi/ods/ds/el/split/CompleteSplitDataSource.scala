package com.dyingbleed.corgi.ods.ds.el.split

import com.dyingbleed.corgi.ods.ds.DataSource
import com.dyingbleed.corgi.core.constant.Mode._
import com.dyingbleed.corgi.ods.core.PartitionStrategy.{NONE, PARTITION_COLUMN, PRIMARY_KEY}
import com.dyingbleed.corgi.ods.ds.el.CompleteDataSource
import com.google.inject.Inject
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2019/1/8.
  */
private[ods] abstract class CompleteSplitDataSource extends DataSource with Splitter {

  @Inject
  var completeDataSource: CompleteDataSource = _

  private lazy val satisfyPKRangeSplit = tableMeta.pk.size == 1 && tableMeta.pk.last.isNumber

  private lazy val satisfyPKHashSplit = tableMeta.pk.nonEmpty && tableMeta.pk.forall(col => col.isNumber || col.isString)

  private lazy val satisfyPartitionSplit = conf.mode == UPDATE || conf.mode == APPEND || (conf.mode == COMPLETE && conf.partitionColumns.length > 1)

  override def canSplit: Boolean = satisfyPKRangeSplit || satisfyPKHashSplit || satisfyPartitionSplit

  override def loadSourceDF: DataFrame = {
    conf.partitionStrategy match {
      case Some(PRIMARY_KEY) => {
        if (satisfyPKRangeSplit) loadPKRangeSplitDF
        else if (satisfyPKHashSplit) loadPKHashSplitDF
        else completeDataSource.loadSourceDF
      }
      case Some(PARTITION_COLUMN) => {
        if (satisfyPartitionSplit) loadPartitionSplitDF
        else completeDataSource.loadSourceDF
      }
      case Some(NONE) => {
        completeDataSource.loadSourceDF
      }
      case None => {
        if (satisfyPKRangeSplit) loadPKRangeSplitDF
        else if (satisfyPKHashSplit) loadPKHashSplitDF
        else if (satisfyPartitionSplit) loadPartitionSplitDF
        else completeDataSource.loadSourceDF
      }
    }
  }
}
