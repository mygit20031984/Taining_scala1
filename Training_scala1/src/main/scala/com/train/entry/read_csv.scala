package com.train.entry

import com.train.session.DemoSparkSession

object read_csv {
  
  def main(args: Array[String]): Unit = {
    
    println("Starting the entry point --------------->")
    
    val spark = DemoSparkSession.spark
    import spark.implicits._ 
    
    
    val a = spark.read.option("header", "true").csv("emp_data1.csv").alias("a"); 
    a.show()
    
    val b = spark.read.option("header", "true").csv("emp_data2.csv").alias("b"); 
    b.show()
    
    val diff = a.join(b, Seq("ID"), "right_outer").filter($"a.value" isNull).drop($"a.value")
    diff.show
    
    diff.write.csv("diff_emp_data.csv")
    
    println("Finishing the entry point --------------->")
    
  }
  
}