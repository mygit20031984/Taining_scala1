package com.train.session

import org.apache.spark.sql.SparkSession

object DemoSparkSession  {
  
    def spark: SparkSession = {
    val session = SparkSession.builder().appName("Training").master("local").getOrCreate()
    session
    
    }
  
}