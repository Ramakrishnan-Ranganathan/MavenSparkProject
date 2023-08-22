package org.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TestcaseMethods {

  def Unique_Check(input: DataFrame, Field: String): Int = {
    val output = input.groupBy(Field).count().where(col("count") > 1).count().toInt
    output
  }

}
