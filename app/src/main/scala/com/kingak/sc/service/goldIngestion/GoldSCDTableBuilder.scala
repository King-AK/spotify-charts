package com.kingak.sc.service.goldIngestion

import com.fasterxml.uuid.Generators
import com.kingak.sc.model.{GoldSpotifyArtistData, SilverSpotifyChartData}
import com.kingak.sc.utils.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scopt.{OParser, OParserBuilder}

object GoldSCDTableBuilder extends SparkSessionProvider with LazyLogging {

  case class Config(
      inputPath: String = "",
      outputPath: String = "",
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
        .validate(x =>
          if (new java.io.File(x).exists) success
          else failure("input file does not exist")
        )
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

}
