package my.example.domain

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

trait SparkTestApp {
  implicit val spark: SparkSession = new sql.SparkSession
    .Builder()
    .appName("TestApp")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
}
