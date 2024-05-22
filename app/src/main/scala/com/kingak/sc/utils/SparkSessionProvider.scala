package com.kingak.sc.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  /*
   * Provides a SparkSession
   */
  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkSessionProvider")
    .config("spark.master", "local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.jars.packages", "io.delta:delta-core_2.13:2.4.0")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
      "spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
    )
    .getOrCreate()

}
