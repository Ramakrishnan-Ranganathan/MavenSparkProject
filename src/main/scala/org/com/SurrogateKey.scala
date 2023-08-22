package org.com
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}


object SurrogateKey {

  def main(args: Array[String]): Unit = {

//    import spark.implicits._
//    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
//    val df = spark.sparkContext.parallelize(Seq(("Databricks", 20000), ("Spark", 100000), ("Hadoop", 3000))).toDF("word", "count")
//    val wcschema = df.schema
//    val inputRows = df.rdd.zipWithUniqueId.map{
//      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
//    val wcID = spark.createDataFrame(inputRows, StructType(StructField("id", LongType, false) +: wcschema.fields))
//
//    wcID.show()

  }

}
