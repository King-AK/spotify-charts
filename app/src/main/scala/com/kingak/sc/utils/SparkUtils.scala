package com.kingak.sc.utils

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.{DeltaTable, DeltaTableBuilder}
import org.apache.spark.sql.Dataset

object SparkUtils extends SparkSessionProvider with LazyLogging {

  def createDeltaTableIfNotExists(
      path: String,
      ds: Dataset[_],
      clusterBy: Option[Seq[String]] = None
  ): Unit = {
    if (new java.io.File(path).exists) {
      assert(DeltaTable.isDeltaTable(path))
      logger.info(s"Delta table already exists at path ${path}")
    } else {
      logger.info(
        s"Delta table does not exist, creating new Delta table at path ${path}"
      )

      val schema = ds.schema
      val dtBuilder: DeltaTableBuilder = schema.foldLeft(DeltaTable.create()) {
        (dt, field) =>
          dt.addColumn(field.name, field.dataType.typeName, field.nullable)
      }

      {
        clusterBy match {
          case Some(clusterByCols) =>
            logger.info(
              s"Clustering new Delta Table by columns: ${clusterByCols.mkString(", ")}"
            )
            dtBuilder.clusterBy(clusterByCols: _*)
          case None => dtBuilder
        }
      }.location(path).execute()

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
