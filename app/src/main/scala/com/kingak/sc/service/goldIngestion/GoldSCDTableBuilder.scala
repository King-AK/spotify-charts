package com.kingak.sc.service.goldIngestion

import com.fasterxml.uuid.Generators
import com.kingak.sc.model.com.kingak.sc.model.GoldSpotifySongData
import com.kingak.sc.model.{
  GoldSpotifyArtistData,
  GoldSpotifyChartData,
  GoldSpotifyRelationshipData,
  SilverSpotifyChartData
}
import com.kingak.sc.utils.SparkSessionProvider
import com.kingak.sc.utils.SparkUtils.{
  batchUpsertToDelta,
  createDeltaTableIfNotExists
}
import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scopt.{OParser, OParserBuilder}

object GoldSCDTableBuilder extends SparkSessionProvider with LazyLogging {

  case class Config(
      inputPath: String = "",
      chartDataOutputPath: String = "",
      artistDataOutputPath: String = "",
      songDataOutputPath: String = "",
      relationshipDataOutputPath: String = "",
      checkpointPath: Option[String] = None
  )

  val builder: OParserBuilder[Config] = OParser.builder[Config]
  val argParser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("GoldSCDTableBuilder"),
      head("GoldSCDTableBuilder", "0.1"),
      opt[String]('i', "input")
        .required()
        .valueName("<input>")
        .action((x, c) => c.copy(inputPath = x))
        .text("input specifies the path to the input file or directory"),
      opt[String]("chart-data-output")
        .required()
        .valueName("<chart-data-output>")
        .action((x, c) => c.copy(chartDataOutputPath = x))
        .text("chart-data-output specifies the path to write the chart data"),
      opt[String]("artist-data-output")
        .required()
        .valueName("<artist-data-output>")
        .action((x, c) => c.copy(artistDataOutputPath = x))
        .text("artist-data-output specifies the path to write the artist data"),
      opt[String]("song-data-output")
        .required()
        .valueName("<song-data-output>")
        .action((x, c) => c.copy(songDataOutputPath = x))
        .text("song-data-output specifies the path to write the song data"),
      opt[String]("relationship-data-output")
        .required()
        .valueName("<relationship-data-output>")
        .action((x, c) => c.copy(relationshipDataOutputPath = x))
        .text(
          "relationship-data-output specifies the path to write the relationship data"
        ),
      opt[String]('c', "checkpoint")
        .optional()
        .valueName("<checkpoint>")
        .action((x, c) => c.copy(checkpointPath = Some(x)))
        .text("checkpoint specifies the path to write the checkpoint files")
    )
  }

  import spark.implicits._

  private def buildUUID(column: String): String = {
    Generators.nameBasedGenerator().generate(column).toString
  }
  private val buildUUIDUDF: UserDefinedFunction = udf(buildUUID _)

  def goldTransformArtistData(
      ds: Dataset[SilverSpotifyChartData]
  ): Dataset[GoldSpotifyArtistData] = {
    ds
      .select("artists")
      .withColumn(
        "artist",
        explode($"artists")
      )
      .withColumn("artistUUID", buildUUIDUDF($"artist"))
      .drop("artists")
      .distinct
      .as[GoldSpotifyArtistData]
  }

  def goldTransformChartData(
      ds: Dataset[SilverSpotifyChartData]
  ): Dataset[GoldSpotifyChartData] = {
    ds.select(
      "id",
      "title",
      "rank",
      "date",
      "artists",
      "url",
      "region",
      "chart",
      "trend",
      "streams",
      "track_id",
      "album",
      "popularity",
      "duration_ms",
      "explicit",
      "release_date",
      "available_markets",
      "af_danceability",
      "af_energy",
      "af_key",
      "af_loudness",
      "af_mode",
      "af_speechiness",
      "af_acousticness",
      "af_instrumentalness",
      "af_liveness",
      "af_valence",
      "af_tempo",
      "af_time_signature"
    ).withColumn("songUUID", buildUUIDUDF($"track_id"))
      .as[GoldSpotifyChartData]
  }
  def goldTransformSongData(
      ds: Dataset[SilverSpotifyChartData]
  ): Dataset[GoldSpotifySongData] = {
    ds.select(
      "title",
      "url",
      "track_id",
      "album",
      "duration_ms",
      "explicit",
      "release_date",
      "available_markets",
      "af_danceability",
      "af_energy",
      "af_key",
      "af_loudness",
      "af_mode",
      "af_speechiness",
      "af_acousticness",
      "af_instrumentalness",
      "af_liveness",
      "af_valence",
      "af_tempo",
      "af_time_signature"
    ).withColumn("songUUID", buildUUIDUDF($"track_id"))
      .distinct
      .as[GoldSpotifySongData]
  }
  def goldTransformRelationshipData(
      ds: Dataset[SilverSpotifyChartData]
  ): Dataset[GoldSpotifyRelationshipData] = {
    ds
      .select("artists", "track_id")
      .withColumn(
        "artist",
        explode($"artists")
      )
      .withColumn("sourceUUID", buildUUIDUDF($"artist"))
      .withColumn("targetUUID", buildUUIDUDF($"track_id"))
      .withColumn("relationshipType", lit("SINGS"))
      .drop("artists", "track_id", "artist")
      .distinct
      .as[GoldSpotifyRelationshipData]
  }

  private def processSilverChartBatch[U](
      batchDS: Dataset[SilverSpotifyChartData],
      outputPath: String,
      mergeCondition: String,
      batchId: Long,
      f: Dataset[SilverSpotifyChartData] => Dataset[U]
  ): Unit = {
    val goldDF = batchDS.transform(f)
    val dt: DeltaTable = {
      createDeltaTableIfNotExists(outputPath, goldDF)
      DeltaTable.forPath(spark, outputPath)
    }
    batchUpsertToDelta(dt)(mergeCondition)(goldDF, batchId)
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        val checkpointPath = config.checkpointPath.getOrElse(
          s"${config.chartDataOutputPath}/_checkpoint"
        )

        val silverSpotifyDS: Dataset[SilverSpotifyChartData] = spark.readStream
          .format("delta")
          .load(config.inputPath)
          .as[SilverSpotifyChartData]

        silverSpotifyDS.writeStream
          .foreachBatch {
            (batchDS: Dataset[SilverSpotifyChartData], batchId: Long) =>
              val goldSpotifyArtistDataMergeCondition: String =
                "existing.artistUUID = updates.artistUUID"
              processSilverChartBatch(
                batchDS,
                config.artistDataOutputPath,
                goldSpotifyArtistDataMergeCondition,
                batchId,
                goldTransformArtistData
              )

              val goldSpotifyChartDataMergeCondition: String =
                "existing.songUUID = updates.songUUID AND existing.date = updates.date"
              processSilverChartBatch(
                batchDS,
                config.chartDataOutputPath,
                goldSpotifyChartDataMergeCondition,
                batchId,
                goldTransformChartData
              )

              val goldSpotifySongDataMergeCondition: String =
                "existing.songUUID = updates.songUUID"
              processSilverChartBatch(
                batchDS,
                config.songDataOutputPath,
                goldSpotifySongDataMergeCondition,
                batchId,
                goldTransformSongData
              )

              val goldSpotifyRelationshipDataMergeCondition: String =
                "existing.sourceUUID = updates.sourceUUID AND existing.targetUUID = updates.targetUUID AND existing.relationshipType = updates.relationshipType"
              processSilverChartBatch(
                batchDS,
                config.relationshipDataOutputPath,
                goldSpotifyRelationshipDataMergeCondition,
                batchId,
                goldTransformRelationshipData
              )
          }
          .option("checkpointLocation", checkpointPath)
          .trigger(Trigger.AvailableNow())
          .start()
          .awaitTermination()

      case _ =>
        logger.error("Invalid arguments")
    }
  }

}
