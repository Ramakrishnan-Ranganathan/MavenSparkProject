package org.com

import org.apache.spark.sql.SparkSession
import org.com.CustomerDataLoad.LoadCustomerData

object DemoDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val customerDataPath = "src/main/resources/Customer.csv"
    val customerData = LoadCustomerData(spark, customerDataPath)
    customerData.show()

  }

}
