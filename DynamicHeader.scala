package com.ramesh.spark

import org.apache.spark.sql.SparkSession
import scala.io.Source

object DynamicHeader {
  def main(array:Array[String])
  {
    val spark=SparkSession.builder().appName("Dynamic Header").master("local[*]").getOrCreate()
    val line=Source.fromFile("/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb2/ramesh/2018 life/hadoop/SparkExamples/Sample_Header.txt").getLines
    val columns=line.flatMap(col => col.split(",")).toSeq
    val fileDataDF=spark.read.csv("/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb2/ramesh/2018 life/hadoop/SparkExamples/sample_data.txt")
    val fileColDF=fileDataDF.toDF(columns:_*)
    fileColDF.select("*").show 
  
  }
  
}