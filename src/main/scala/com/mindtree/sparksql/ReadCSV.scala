package com.mindtree.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadCSV {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReadCSV").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val data = "C:\\Users\\dntadmin\\Downloads\\BigData\\Dataset\\option-chain-ED-BANKNIFTY-25-Nov-2021.csv"
    val df = spark.read.format(("csv")).option("header","true").load(data)
    df.show(false)
    //df.printSchema()
    //---------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}