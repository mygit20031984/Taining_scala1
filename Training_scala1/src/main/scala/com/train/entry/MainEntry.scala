package com.train.entry

import com.train.session.DemoSparkSession

object MainEntry {
  
  def main(args: Array[String]): Unit = {
    
    println("Starting the entry point --------------->")
    
    val spark = DemoSparkSession.spark
    import spark.implicits._ 
    
    
    val a = spark.read.option("header", "true").csv("emp_data1.csv").alias("a"); 
    a.show()
    
    a.createOrReplaceTempView("emp_data1")
    val sqlDF = spark.sql("SELECT * FROM emp_data1 where Marks > 90")
    sqlDF.show()
    
    val b = spark.read.option("header", "true").csv("emp_data2.csv").alias("b"); 
    //b.show()
    
    //a.except(b).show()
    println("Finishing the entry point --------------->")
    
  }
  
  
}