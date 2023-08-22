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
import java.time.LocalDate


object TestFUnctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    import spark.implicits._
    val simpleData = Seq(("ramakrishnan.r.1maersk.com","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.show()

    val df1 = df.filter(col("employee_name") === regexp_extract(col("employee_name"),"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$",0))
    df1.show()

    df.withColumn("CurrentDate", current_date())
      .withColumn("AddMonths", add_months(current_date(), -12)).show()



//    def Unique_Check(input: DataFrame, Field: String) : String = {
//      val output = input.groupBy(Field).count().where(col("count") > 1).count().toInt
//      output match {
//        case 0 => "PASS"
//        case _ => "FAIL"
//      }
//    }
//
//    val TestcaseData = Seq((1,"Uniqueness Check on Dimsite","Unique_Check","WnD.DimSite","Site_ID","NA","Site Master","2022-04-04",1),
//      (2,"NULL Check on Dimsite","Null_Check","WnD.DimSite","Site_ID","NA","Site Master","2022-04-04",1))
//
//    val TescaseDF = TestcaseData.toDF("TestcaseID", "Testcase", "TestcaseMethodName", "TableName", "ColumnNames", "ExpectedData", "Product", "TestCreationDate", "TestFlag")
//
//    val listDF = TescaseDF.where(col("TestFlag") === lit(1)).collect()
//
//    val method = listDF.map{f =>
//      f.get(2) match {
//        case "Unique_Check" => (f.get(0).toString,"Pass",LocalDate.now().toString)
//        case "Null_Check" => (f.get(0).toString,"Pass",LocalDate.now().toString)
//      }
//    }

















  }

}
