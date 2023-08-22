package org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object CumulativeSumExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(("Banana",1000,"USA","2023-04-12"),
      ("Banana",1000,"USA","2022-09-18"),
      ("Carrots",1500,"USA","2023-01-01"),
      ("Beans",1600,"USA","2023-06-01"),
      ("Orange",2000,"USA","2022-09-18"),
      ("Orange",2000,"USA","2023-01-01"),
      ("Banana",400,"China","2023-04-12"),
      ("Carrots",1200,"China","2023-01-01"),
      ("Beans",1500,"China","2022-09-18"),
      ("Orange",4000,"China","2023-04-12"),
      ("Banana",2000,"Canada","2023-01-01"),
      ("Carrots",2000,"Canada","2023-06-01"),
      ("Beans",2000,"Mexico","2023-06-01"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country","sale_date")
    df.show()

    val windowSpec = Window.partitionBy(col("Country"))
                          .orderBy(col("sale_date")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val output = df.withColumn("running_total", sum(col("Amount")).over(windowSpec) )
    output.show()

  }

}
