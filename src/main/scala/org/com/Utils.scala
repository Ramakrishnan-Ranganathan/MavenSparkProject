package org.com

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  def readfile(spark: SparkSession, path: String, fileFormat: String): DataFrame = {
    spark.read.format(fileFormat).load(path)
  }

}
