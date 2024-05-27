package com.kingak.sc.service

import com.kingak.sc.model.com.kingak.sc.model.GoldSpotifySongData
import com.kingak.sc.model.{
  GoldSpotifyArtistData,
  GoldSpotifyChartData,
  GoldSpotifyRelationshipData,
  SilverSpotifyChartData
}
import com.kingak.sc.service.goldIngestion.GoldSCDTableBuilder.{
  goldTransformArtistData,
  goldTransformChartData,
  goldTransformRelationshipData,
  goldTransformSongData
}
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

  test(
    "Transforms should be applied to SilverSpotifyChartData to create GoldSpotifyChartData"
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
    val chartDataDF: Dataset[GoldSpotifyChartData] =
      df.transform(goldTransformChartData)

    assertResult(30000)(chartDataDF.count)
  }

  test(
    "Transforms should be applied to SilverSpotifyChartData to create GoldSpotifySongData"
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
    val spotifySongDataDF: Dataset[GoldSpotifySongData] =
      df.transform(goldTransformSongData)

    // assert song UUIDS
    val songUUIDs = spotifySongDataDF
      .filter(
        $"track_id".isin(
          "2r9homVqy3ntb7iOoROL88",
          "2xNZeeqmwMPVKnD7FSQfgt",
          "5N8nNuTmIzkZOfcxXlygUw"
        )
      )
      .orderBy("track_id")
      .collect()
      .map(_.songUUID)

    assertResult(
      Array(
        "4c5017aa-ceb3-5cdf-a01e-65353ac45fe6", // 2r9homVqy3ntb7iOoROL88
        "600ed63d-d84c-5578-be21-ea4f380d6997", // 2xNZeeqmwMPVKnD7FSQfgt
        "fb5c23ea-ef1e-5c1e-9616-843a9fefe43a" // 5N8nNuTmIzkZOfcxXlygUw
      )
    )(songUUIDs)
  }

  test(
    "Transforms should be applied to SilverSpotifyChartData to create GoldSpotifyRelationshipData"
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
    val relationshipDataDF: Dataset[GoldSpotifyRelationshipData] =
      df.transform(goldTransformRelationshipData)

    // display some data
    assertResult(7021)(relationshipDataDF.count)
  }

}
