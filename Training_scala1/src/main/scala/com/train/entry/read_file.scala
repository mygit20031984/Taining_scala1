package com.train.entry

import org.apache.spark.sql.SparkSession
import com.train.session.DemoSparkSession
import org.apache.spark.sql.DataFrame

object read_file {
  
      val spark = DemoSparkSession.spark
      import spark.implicits._ 
    
      
      
      def read_csv(file1:String): DataFrame = { 
          val c = spark.read.option("header", "true").csv(file1)
          return c
    
   }
        
  
}