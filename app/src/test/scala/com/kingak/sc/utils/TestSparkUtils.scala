package com.kingak.sc.utils

import com.kingak.sc.testDataModel.TestData
import com.kingak.sc.utils.SparkUtils.createDeltaTableIfNotExists
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.Trigger
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

@RunWith(classOf[JUnitRunner])
class TestSparkUtils extends AnyFunSuite with BeforeAndAfterEach {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  val outputDirectory = "/tmp/TestSparkUtils"
  def deleteOutputDirectory(): Unit = {
    val outputDir = new Directory(new File(outputDirectory))
    outputDir.deleteRecursively()
  }
  override def beforeEach(): Unit = {
    if (new File(outputDirectory).exists)
      deleteOutputDirectory()
    new File(outputDirectory).mkdirs()
  }

  override def afterEach(): Unit = {
    deleteOutputDirectory()
  }

  import spark.implicits._

  test(
    "createDeltaTableIfNotExists should create a delta table if it does not exist when a streaming dataframe is provided"
  ) {

    val tableName = "test_table"
    val tablePath = s"$outputDirectory/$tableName"
    val checkpointPath = s"$outputDirectory/$tableName/_checkpoint"
    val testDataBatch1OriginPath = "src/test/resources/TestData/test_data_1.csv"
    val testDataPath = s"$outputDirectory/testData"
    // copy test data to test directory
    new File(testDataPath).mkdirs()
    Files.copy(
      new File(testDataBatch1OriginPath).toPath,
      new File(s"$testDataPath/test_data_1.csv").toPath
    )

    val schema = Encoders.product[TestData].schema

    val testDS = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(testDataPath)
      .as[TestData]

    createDeltaTableIfNotExists(tablePath, testDS)

    // confirm table exists
    assert(new File(tablePath).exists)
    // confirm table is empty as no data should be written on creation
    val df = spark.read.format("delta").load(tablePath)
    assertResult(0)(df.count)
  }

  test(
    "batchUpsertToDelta should upsert streaming data as new batches are provided"
  ) {

    val tableName = "test_table"
    val tablePath = s"$outputDirectory/$tableName"
    val checkpointPath = s"$outputDirectory/$tableName/_checkpoint"
    val testDataBatch1OriginPath = "src/test/resources/TestData/test_data_1.csv"
    val testDataBatch2OriginPath = "src/test/resources/TestData/test_data_2.csv"
    val testDataPath = s"$outputDirectory/testData"
    // copy test data to test directory
    new File(testDataPath).mkdirs()
    Files.copy(
      new File(testDataBatch1OriginPath).toPath,
      new File(s"$testDataPath/test_data_1.csv").toPath
    )

    val schema = Encoders.product[TestData].schema

    val testDS = spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(testDataPath)
      .as[TestData]

    createDeltaTableIfNotExists(tablePath, testDS)
    val dt: DeltaTable = DeltaTable.forPath(tablePath)
    assert(dt.toDF.count == 0)

    // write first batch of test data
    val mergeCondition = "existing.A = updates.A"
    testDS.writeStream
      .foreachBatch(SparkUtils.batchUpsertToDelta(dt)(mergeCondition) _)
      .outputMode("update")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.AvailableNow())
      .start()
      .awaitTermination()
    assert(dt.toDF.count == 3)

    // copy batch 2 test data to test directory
    Files.copy(
      new File(testDataBatch2OriginPath).toPath,
      new File(s"$testDataPath/test_data_2.csv").toPath
    )

    // read and write second batch of test data
    spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(testDataPath)
      .as[TestData]
      .writeStream
      .foreachBatch(SparkUtils.batchUpsertToDelta(dt)(mergeCondition) _)
      .outputMode("update")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.AvailableNow())
      .start()
      .awaitTermination()

    val df = spark.read.format("delta").load(tablePath)
    assertResult(5)(df.count)
  }

}
