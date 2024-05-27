package com.kingak.sc.service

import com.kingak.sc.model.BronzeSpotifyChartData
import com.kingak.sc.utils.SparkSessionProvider
import org.apache.spark.sql.{Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestBronzeSCDTableBuilder extends AnyFunSuite with BeforeAndAfterEach {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  import spark.implicits._

  test(
    "BronzeSpotifyChartData case class should have appropriate schema to read CSV"
  ) {
    val schema = Encoders.product[BronzeSpotifyChartData].schema

    val testDataPath = "src/test/resources/RawSpotifyChartData/"

    val df = spark.read
      .schema(schema)
      .option("multiline", "true")
      .option("header", "true")
      .csv(testDataPath)
      .as[BronzeSpotifyChartData]

    assertResult(30000)(df.count)
  }

}
