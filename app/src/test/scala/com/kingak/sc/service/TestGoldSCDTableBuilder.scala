package com.kingak.sc.service

import com.kingak.sc.model.{
  BronzeSpotifyChartData,
  GoldSpotifyArtistData,
  SilverSpotifyChartData
}
import com.kingak.sc.service.goldIngestion.GoldSCDTableBuilder.goldTransformArtistData
import com.kingak.sc.service.silverIngestion.SilverSCDTableBuilder.silverTransform
import com.kingak.sc.utils.SparkSessionProvider
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestGoldSCDTableBuilder extends AnyFunSuite with BeforeAndAfterEach {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  import spark.implicits._

  test(
    "Transforms should be applied to SilverSpotifyChartData to create GoldSpotifyArtistData"
  ) {
    val schema = Encoders.product[SilverSpotifyChartData].schema
    val testDataPath = "src/test/resources/SilverSpotifyChartData"

    val df = spark.read
      .schema(schema)
      .option("format", "delta")
      .option("path", testDataPath)
      .load()
      .as[SilverSpotifyChartData]

    assertResult(30000)(df.count)

    // Apply transformations to create GoldSCDTables
    val artistDataDF: Dataset[GoldSpotifyArtistData] =
      df.transform(goldTransformArtistData)

    // assert UUIDs for some artists
    val artistUUIDs = artistDataDF
      .filter($"artist".isin("Major Lazer", "BTS", "Lil Wayne"))
      .orderBy("artist")
      .collect()
      .map(_.artistUUID)
    assertResult(
      Array(
        "24c4c647-549c-53ac-a922-0100a26fa595", // BTS
        "170d97d3-39c7-565e-9199-cf72dc40e1b3", // Lil Wayne
        "cbcc39b7-6017-576c-98f7-0e68d34e8015" // Major Lazer
      )
    )(artistUUIDs)
  }

}
