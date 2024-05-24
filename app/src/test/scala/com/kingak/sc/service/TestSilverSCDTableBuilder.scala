package com.kingak.sc.service

import com.kingak.sc.model.{BronzeSpotifyChartData, SilverSpotifyChartData}
import com.kingak.sc.service.silverIngestion.SilverSCDTableBuilder.silverTransform
import com.kingak.sc.utils.SparkSessionProvider
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSilverSCDTableBuilder extends AnyFunSuite with BeforeAndAfterEach {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  import spark.implicits._

  test(
    "Transforms should be applied to RawSpotifyChartData to create SilverSCDTable"
  ) {
    val schema = Encoders.product[BronzeSpotifyChartData].schema
    val testDataPath = "src/test/resources/BronzeSpotifyChartData"

    val df = spark.read
      .schema(schema)
      .option("format", "delta")
      .option("path", testDataPath)
      .load()
      .as[BronzeSpotifyChartData]

    assertResult(30000)(df.count)

    // Apply transformations to create SilverSCDTable
    val transformedDF: Dataset[SilverSpotifyChartData] =
      df.transform(silverTransform)
    assertResult(30000)(transformedDF.count)
  }

}
