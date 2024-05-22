package com.kingak.sc.basics

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSparkWorks extends AnyFunSuite with BeforeAndAfterAll {

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

  test("Spark Session is initialized") {
    assertResult("hello")(spark.sql("SELECT 'hello'").collect().head.get(0))
  }
}
