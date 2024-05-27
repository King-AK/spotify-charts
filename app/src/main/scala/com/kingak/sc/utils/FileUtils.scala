package com.kingak.sc.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import scala.util.Try
import scala.util.matching.Regex

object FileUtils {

  def fileExists(filePath: String): Boolean = {
    val cloudPathPattern: Regex = "(abfss|dbfs):/.*".r
    filePath match {
      case cloudPathPattern(_) =>
        Try {
          dbutils.fs.ls(filePath).nonEmpty
        }.getOrElse(false)
      case _ =>
        new java.io.File(filePath).exists
    }
  }

}
