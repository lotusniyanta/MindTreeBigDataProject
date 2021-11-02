package com.mindtree.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadTextfile {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReadTextfile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val data = "C:\\Users\\dntadmin\\Downloads\\BigData\\Dataset\\Candidatedetails.txt"
    val df = spark.read.format(("text")).option("header","true").load(data)

    val df2 = df.select(split(col("value")," ").getItem(0).as("Name1"),
      split(col("value")," ").getItem(1).as("Age"))


    df2.createOrReplaceTempView("p1")
    val results = spark.sql("select count(*)  from p1 where  Name1 like '%raj%' ")
    //results.show()

    val baserdd = sc.textFile(data)
    val hedr = baserdd.first()
    val rdd1 = hedr.filter(x=> x != x)
    //rdd1.foreach(print)
    val datardd = baserdd.map(x => x.split(' '))
    val datardd1 = datardd.first()
    //datardd.foreach(print) //not printing the value
    val df1 = datardd.toDF("column1")
    df1.show(truncate = false)
    df1.createOrReplaceTempView("tempview")
    val res = spark.sql("Select count(*) from tempview  where  column1 like '%niraj%'") //not working
    
    res.show()
    //---------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}