package org.com

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
import scala.collection.mutable.Set


object DateFunctionUdf {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    val data = Seq(("2012-12-21"),("2012-12-22"),("2012-12-23"),("2012-12-25"),("2012-12-25"),
      ("2012-12-26"),("2012-12-27"),("2012-12-28"),("2012-12-29"),("2012-12-30"),("2012-12-31"))

    import spark.implicits._
    val df = data.toDF("date")

    val df1 = df.withColumn("year", year(col("date")))
      .withColumn("monthNumber", month(col("date")))

    val weekOfYearFUnction = (str: String) => {
      val date_formate = new SimpleDateFormat("w")
      val date_formate1 = new SimpleDateFormat("yyyy-MM-dd")
      val date_formate2 = new SimpleDateFormat("yyyy")
      val date_formate3 = new SimpleDateFormat("MM")
      val year = date_formate2.format(date_formate1.parse(str)).toInt
      val month = date_formate3.format(date_formate1.parse(str)).toInt
      val start = LocalDate.of(year,12,1).toEpochDay
      val end = LocalDate.of(year,12,31).toEpochDay
      val temp_output = date_formate.format(date_formate1.parse(str)).toInt
      var temp_set = Set.empty[Int]
      if (temp_output == 1 && month == 12) {
        val dates = (start to end).map(LocalDate.ofEpochDay(_).toString).toArray
        for (x <- dates) {
          temp_set += date_formate.format(date_formate1.parse(x)).toInt
        }
        temp_set.max + 1
      }else{
        temp_output
      }

    }



    val UdfToGetWeek = udf(weekOfYearFUnction)
    val WeekOfYearDF = df1.withColumn("Week",UdfToGetWeek(col("date")))

    WeekOfYearDF.show()











  }

}
