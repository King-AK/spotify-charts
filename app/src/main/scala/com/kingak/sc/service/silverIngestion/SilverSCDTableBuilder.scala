package com.kingak.sc.service.silverIngestion

import com.kingak.sc.model.{BronzeSpotifyChartData, SilverSpotifyChartData}
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

object SilverSCDTableBuilder extends SparkSessionProvider with LazyLogging {

  case class Config(
      inputPath: String = "",
      outputPath: String = "",
      checkpointPath: Option[String] = None
  )

  val builder: OParserBuilder[Config] = OParser.builder[Config]
  val argParser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("SilverSCDTableBuilder"),
      head("SilverSCDTableBuilder", "0.1"),
      opt[String]('i', "input")
        .required()
        .valueName("<input>")
        .action((x, c) => c.copy(inputPath = x))
        .text("input specifies the path to the input file or directory"),
      opt[String]('o', "output")
        .required()
        .valueName("<output>")
        .action((x, c) => c.copy(outputPath = x))
        .text("output specifies the path to write the delta lake output"),
      opt[String]('c', "checkpoint")
        .optional()
        .valueName("<checkpoint>")
        .action((x, c) => c.copy(checkpointPath = Some(x)))
        .text("checkpoint specifies the path to write the checkpoint files")
    )
  }

  import spark.implicits._

  private def buildArtistsColumn(artist: String): Array[String] = {
    artist.split(",").map(_.trim)
  }
  private val buildArtistsUDF: UserDefinedFunction = udf(buildArtistsColumn _)

  def silverTransform(
      ds: Dataset[BronzeSpotifyChartData]
  ): Dataset[SilverSpotifyChartData] = {
    ds
      .withColumn(
        "available_markets",
        split(regexp_replace($"available_markets", "[\\[\\]'\\s]", ""), ",")
      )
      .withColumn("artists", buildArtistsUDF($"artist"))
      .withColumn("streams", $"streams".cast("long"))
      .withColumn("duration_ms", $"duration_ms".cast("long"))
      .withColumn("af_key", $"af_key".cast("int"))
      .withColumn("af_mode", $"af_mode".cast("int"))
      .withColumn("af_time_signature", $"af_time_signature".cast("int"))
      .drop("artist")
      .as[SilverSpotifyChartData]
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        val ds: Dataset[SilverSpotifyChartData] = spark.readStream
          .format("delta")
          .load(config.inputPath)
          .as[BronzeSpotifyChartData]
          .transform(silverTransform)

        val checkpointLocation =
          config.checkpointPath.getOrElse(config.outputPath + "/_checkpoint")
        val dt: DeltaTable = {
          createDeltaTableIfNotExists(config.outputPath, ds)
          DeltaTable.forPath(spark, config.outputPath)
        }

        val mergeCondition: String =
          "existing.date = updates.date AND existing.track_id = updates.track_id"

        ds.writeStream
          .foreachBatch(batchUpsertToDelta(dt)(mergeCondition) _)
          .outputMode("update")
          .option("checkpointLocation", checkpointLocation)
          .trigger(Trigger.AvailableNow())
          .start()
          .awaitTermination()

      case _ =>
        logger.error("Failed to parse command line arguments")
        sys.exit(1)
    }
  }

}
