package com.kingak.sc.basics

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestCSVParse extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // initialize spark session
    spark = SparkSession
      .builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // close spark session
    spark.stop()
  }

  test("Spark read Spotify CSV File into DataFrame") {
    // specify path to test data
    val path =
      "src/test/resources/RawSpotifyChartData/file_1.csv"
    // confirm file exists at path
    // print working directory
    println(System.getProperty("user.dir"))
    assert(new java.io.File(path).exists)

    // read CSV file into DataFrame
    val df = spark.read
      .option("multiline", "true")
      .option("header", "true")
      .csv(path)
    assertResult(10000)(df.count)
  }

  test("Spark read Spotify CSV Directory into DataFrame") {
    // specify path to test data
    val path =
      "src/test/resources/RawSpotifyChartData"
    // confirm file exists at path
    assert(new java.io.File(path).exists)

    // read CSV directory into DataFrame
    val df = spark.read
      .option("multiline", "true")
      .option("header", "true")
      .csv(path)
    assertResult(30000)(df.count)
  }

}
