package com.dyingbleed.corgi.ods.ds.el.split

import com.dyingbleed.corgi.ods.ds.DataSource
import com.dyingbleed.corgi.ods.ds.el.IncrementalDataSource
import com.dyingbleed.corgi.ods.core.PartitionStrategy._
import com.google.inject.Inject
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2019/1/8.
  */
private[ods] abstract class IncrementalSplitDataSource extends DataSource with Splitter {

  @Inject
  var incrementalDataSource: IncrementalDataSource = _

  private lazy val satisfyPKRangeSplit = tableMeta.pk.size == 1 && tableMeta.pk.last.isNumber

  private lazy val satisfyPKHashSplit = tableMeta.pk.nonEmpty && tableMeta.pk.forall(col => col.isNumber || col.isString)

  private lazy val satisfyPartitionSplit = conf.partitionColumns.length > 1

  override def canSplit: Boolean = satisfyPKRangeSplit || satisfyPKHashSplit || satisfyPartitionSplit

  override def loadSourceDF: DataFrame = {
    conf.partitionStrategy match {
      case Some(PRIMARY_KEY) => {
        if (satisfyPKRangeSplit) loadPKRangeSplitDF
        else if (satisfyPKHashSplit) loadPKHashSplitDF
        else incrementalDataSource.loadSourceDF
      }
      case Some(PARTITION_COLUMN) => {
        if (satisfyPartitionSplit) loadPartitionSplitDF
        else incrementalDataSource.loadSourceDF
      }
      case Some(NONE) => {
        incrementalDataSource.loadSourceDF
      }
      case None => {
        if (satisfyPKRangeSplit) loadPKRangeSplitDF
        else if (satisfyPKHashSplit) loadPKHashSplitDF
        else if (satisfyPartitionSplit) loadPartitionSplitDF
        else incrementalDataSource.loadSourceDF
      }
    }
  }

}
