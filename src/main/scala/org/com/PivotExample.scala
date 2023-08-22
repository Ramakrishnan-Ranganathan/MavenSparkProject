package org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")

    df.show()


//    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
//    pivotDF.show()

    val pivotDF = df.groupBy("Product","Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    pivotDF.show()

    val unPivotDF = pivotDF.select($"Product",
      expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico,'USA', USA) as (Country,Total)"))
      .where("Total is not null")
    unPivotDF.show()

  }

}
