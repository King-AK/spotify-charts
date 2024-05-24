package com.kingak.sc.model

import com.kingak.sc.testDataModel.TestData
import com.kingak.sc.utils.SparkSessionProvider
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
class TestSpotifyChartData extends AnyFunSuite with BeforeAndAfterEach {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  import spark.implicits._

  test(
    "SpotifyChartData case class should have appropriate schema to read CSV"
  ) {
    val schema = Encoders.product[SpotifyChartData].schema

    val testDataPath = "src/test/resources/RawSpotifyChartData/file_1.csv"

    val df = spark.read
      .schema(schema)
      .option("multiline", "true")
      .option("header", "true")
      .csv(testDataPath)
      .as[SpotifyChartData]

    assertResult(10000)(df.count)
  }

}
