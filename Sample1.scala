package com.spark.basics
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import org.apache.spark.sql.SQLContext
import scala.io.Codec
import org.apache.spark.sql.SparkSession
object Sample1 {
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

// Get first 5 rows
/*df.head(5) // returns an Array
println("\n")
for(line <- df.head(10)){
  println(line)
}*/

// Get column names
df.columns

// Find out DataTypes
// Print Schema
df.printSchema()

// Describe DataFrame Numerical Columns
df.describe()

// Select columns .transform().action()
//df.select("Volume").show()
df.filter("Close <480 AND High < 484.40").show()
// showing multiple 
df.select("Volume" , "Close").show()


    /* Creating New Columns from existing column

 
val df2 = df.withColumn("HighPlusLow",df("High")-df("Low"))
// Show result
df2.columns
df2.printSchema()

// Recheck Head
df2.head(5)

// Renaming Columns (and selecting some more)
df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show()*/
}
}