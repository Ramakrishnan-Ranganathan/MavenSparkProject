package org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollectListExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(("Banana",1000,"USA"),
      ("Banana",1000,"USA"),
      ("Carrots",1500,"USA"),
      ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),
      ("Orange",2000,"USA"),
      ("Banana",400,"China"),
      ("Carrots",1200,"China"),
      ("Beans",1500,"China"),
      ("Orange",4000,"China"),
      ("Banana",2000,"Canada"),
      ("Carrots",2000,"Canada"),
      ("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")

    df.show()

    val output = df.groupBy(col("Product")).agg(sum("Amount"))
    output.show()

    val output1 = df.groupBy(col("Product")).agg(sum("Amount"),
                    collect_list("Country").alias("list_Country"))
                  .withColumn("element_size", size(col("list_Country")))
    output1.show(false)

    val output2 = df.groupBy(col("Product")).agg(sum("Amount"),collect_set("Country"))
    output2.show(false)


  }

}
