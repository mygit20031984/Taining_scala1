package com.train.entry

import com.train.session.DemoSparkSession
import com.train.entry.read_file

object MainEntry {
  
  def main(args: Array[String]): Unit = {
    
    println("Starting the entry point --------------->")
    
    val spark = DemoSparkSession.spark
    import spark.implicits._ 
    
    
    val DF1 = read_file.read_csv("emp_data1.csv")
    
    
    /*
    val a = spark.read.option("header", "true").csv("emp_data1.csv").alias("a"); 
    a.show()
    
    val dup_df = a.groupBy($"ID",$"NAme",$"Marks").count.filter($"count" > 1).show()
    
    //a.createOrReplaceTempView("emp_data1")
    //val sqlDF = spark.sql("SELECT * FROM emp_data1 where Marks > 90")
    //sqlDF.show()
    
    val b = spark.read.option("header", "true").csv("emp_data2.csv").alias("b"); 
    //b.show()
    
    //a.except(b).show()
    println("Finishing the entry point --------------->")
    */
  }
  
  
}