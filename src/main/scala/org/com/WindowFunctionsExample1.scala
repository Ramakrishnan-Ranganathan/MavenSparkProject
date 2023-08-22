package org.com

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


object WindowFunctionsExample1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
                  .appName("Test")
                  .master("local[*]")
                  .getOrCreate()

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100))

    import spark.implicits._

    val df = simpleData.toDF("employee_name","department","salary")

    val windowConf = Window.partitionBy(col("department"))
    val topSalaryByDepartment = df.withColumn("rownumber", row_number().over(windowConf))
    topSalaryByDepartment.show()

    val rankOutput = df.withColumn("rank", dense_rank().over(windowConf))
    rankOutput.show()

    val leadOutput = df.withColumn("lead", lead(col("salary"),1).over(windowConf))
    leadOutput.show()

    val lagOutput = df.withColumn("lag", lag(col("salary"),1).over(windowConf))
    lagOutput.show()


    val groupOutput = df.groupBy(col("department")).agg(sum("salary"))
    groupOutput.show()

    val sumOutput = df.withColumn("maxSalary", max("salary").over(windowConf))
                      .withColumn("salaryDiff", col("maxSalary") - col("salary"))
    sumOutput.show()

    print(sumOutput.rdd.toDebugString)






  }

}
