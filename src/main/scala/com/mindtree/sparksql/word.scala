
 package com.mindtree.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object word {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("word").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val data = "C:\\Users\\dntadmin\\Downloads\\datasets\\world_bank.json"
    val df = spark.read.format("json").load(data)
    df.printSchema()
    //df.show()

print("***************************************Explode Example1*********************************")
    println("")
    val df1 = df.withColumn("tn",explode($"mjsector_namecode")).
      withColumn("Code",$"tn.code").withColumn("Name",$"tn.name").
      withColumn("Id",monotonicallyIncreasingId()).
      select($"Id",$"Code",$"Name")

    print("*************************************** Explode Example2*********************************")
    println("")
  val df2 = df.withColumn("tn1",explode($"theme_namecode")).
    withColumn("Nm",$"approvalfy").select($"tn1.*",$"Nm")

    //df2.select("*").show()
    print("*************************************** Explode Example3*********************************")
    println("")
    val df3 = df.withColumn("Apfly", $"approvalfy").
      withColumn("Approval_Month",$"board_approval_month").select($"Apfly", $"Approval_Month")
    //df3.select("Apfly","Approval_Month").show(false)
   // df3.select(df3.colRegex("`^.*ap*`")).show()
    //---------------------------------------------------
df3.createOrReplaceTempView("p1")
val results = spark.sql("select name,Nm  from p1")
//results.show()

//----------------------------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}