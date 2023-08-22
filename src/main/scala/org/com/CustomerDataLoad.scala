package org.com
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.com.Utils.readfile
object CustomerDataLoad {

  def LoadCustomerData(spark:SparkSession, path: String) = {
//    val df = spark.read.format("csv").load(Path1)

    val df = readfile(spark, path, "csv")
    df
  }

}
