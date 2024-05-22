package com.kingak.sc.utils

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSparkSessionProvider extends AnyFunSuite with BeforeAndAfterAll {

  object TestSparkSessionProvider extends SparkSessionProvider
  val spark: SparkSession = TestSparkSessionProvider.spark

  test("SparkSessionProvider should provide a SparkSession") {
    assertResult("hello")(spark.sql("SELECT 'hello'").collect().head.get(0))
  }
}
