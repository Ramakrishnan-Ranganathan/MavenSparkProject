package SparkClass1

import org.apache.spark.sql.{functions,SparkSession}
import org.apache.log4j.{Level, Logger}

object ParallelizedRdd {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val data = Array(1,2,3,4,5,6,7,8,9,10)
    val spark = SparkSession.builder()
                  .appName("Test")
                  .master("local[*]")
                  .getOrCreate()
    val rdd = spark.sparkContext.parallelize(data)
    val result = rdd.reduce( (a,b) => a + b )
    println(result)

//    println(rdd.getNumPartitions)
//    val rdd1 = rdd.repartition(4)
//    println(rdd1.getNumPartitions)
//
//    val str = rdd1.toDebugString
//    println(str)


  }

}
