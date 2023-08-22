package SparkClass1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ExternalRdd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()
    val data = spark.sparkContext.textFile("file.///C:/Users/RAR208/OneDrive - Maersk Group/Documents/Spark/DataFiles/SampleData.txt")

  }

}
