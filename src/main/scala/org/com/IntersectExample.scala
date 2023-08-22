package org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IntersectExample {

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

    val data1 = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),("Banana",1000,"USA"))

    import spark.sqlContext.implicits._
    val df1 = data1.toDF("Product","Amount","Country")

    df.select(col("Product")).intersectAll(df1.select(col("Product"))).show()
    df.select(col("Product")).intersect(df1.select(col("Product"))).show()

  }

}
