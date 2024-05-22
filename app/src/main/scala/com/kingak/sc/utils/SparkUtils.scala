package com.kingak.sc.utils

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset}

object SparkUtils extends SparkSessionProvider with LazyLogging {

  val log = logger

  def createDeltaTableIfNotExists(
      path: String,
      checkpointPath: String,
      ds: Dataset[_]
  ): Unit = {
    if (new java.io.File(path).exists) {
      assert(DeltaTable.isDeltaTable(path))
      logger.info(s"Delta table already exists at path ${path}")
    } else {
      logger.info(
        s"Delta table does not exist, creating new Delta table at path ${path}"
      )
      ds.writeStream
        .format("delta")
        .option("path", path)
        .option("checkpointLocation", checkpointPath)
        .trigger(Trigger.AvailableNow())
        .start()
        .awaitTermination()
    }
  }

  def batchUpsertToDelta(
      dt: DeltaTable
  )(mergeCondition: String)(df: Dataset[_], batchId: Long): Unit = {
    dt.as("existing")
      .merge(
        df.toDF.as("updates"),
        mergeCondition
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

}
