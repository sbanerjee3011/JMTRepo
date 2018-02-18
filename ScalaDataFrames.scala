package com.spark.basics
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import org.apache.spark.sql.SQLContext
import scala.io.Codec
import org.apache.spark.sql.SparkSession
object ScalaDataFrames {
   def main(args: Array[String]) {
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("Test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      val df = spark.read.option("header","true").option("inferSchema","true").csv("C:/read/CitiGroup2006_2008")

// Get first 5 rows from the file 
df.head(5) // returns an Array
println("\n")
for(line <- df.head(10)){
  println(line)
}

// Get column names
df.columns

// Find out DataTypes
// Print Schema
df.printSchema()

// Describe DataFrame Numerical Columns
df.describe()

// Select one or more columns for display ...
//df.select("Volume").show()
//df.select("Volume" , "Close").show()

// Apply filters on column data = >Filter 1- 
//df.filter("Close<480 AND High < 484.40").show()


// instead of displaying the result we want to collect these results in scala object  = >Filter 2- 
val High484 = df.filter("High==484.40").collect()
 High484.foreach(println)

}
}