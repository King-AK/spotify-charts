package com.kingak.sc.service

import com.kingak.sc.model.SpotifyChartData
import com.kingak.sc.utils.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Encoders}
import scopt.{OParser, OParserBuilder}

object CSVDataIngestor extends SparkSessionProvider
  with LazyLogging {

  case class Config(
      inputPath: String = "",
      outputPath: String = "",
      checkpointPath: Option[String] = None
  )

  val builder: OParserBuilder[Config] = OParser.builder[Config]
  val argParser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("CSVDataIngestor"),
      head("CSVDataIngestor", "0.1"),
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

  def upsertToDelta(
      dt: DeltaTable
  )(mergeCondition: String)(df: Dataset[_], batchId: Long): Unit = {
    dt.as("existing")
      .merge(
        df.toDF.as("updates"),
        mergeCondition
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // parse command line arguments
    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        // confirm that the input path exists
        assert(new java.io.File(config.inputPath).exists)

        // Build schema using case class
        val schema = Encoders.product[SpotifyChartData].schema

        // Read CSV files in directory into streaming DataSet
        val ds = spark.readStream
          .schema(schema)
          .option("header", "true")
          .csv(config.inputPath)
          .as[SpotifyChartData]

        val checkpointLocation =
          config.checkpointPath.getOrElse(config.outputPath + "/_checkpoint")
        val dt: DeltaTable = {
          if (new java.io.File(config.outputPath).exists) {
            assert(DeltaTable.isDeltaTable(config.outputPath))
          } else {
            logger.info(s"Delta table does not exist, creating new Delta table at path ${config.outputPath}")
            ds.writeStream
              .format("delta")
              .option("path", config.outputPath)
              .option("checkpointLocation", checkpointLocation)
              .trigger(Trigger.AvailableNow())
              .start()
              .awaitTermination()
          }
          DeltaTable.forPath(spark, config.outputPath)
        }

        val mergeCondition = "TODO"

        // Stream upsert to Delta Lake
        ds.writeStream
          .foreachBatch(upsertToDelta(dt)(mergeCondition) _)
          .outputMode("update")
          .option("checkpointLocation", checkpointLocation)
          .trigger(Trigger.AvailableNow())
          .start()

      case _ =>
        logger.error("Failed to parse command line arguments")
        sys.exit(1)
    }
  }

}
