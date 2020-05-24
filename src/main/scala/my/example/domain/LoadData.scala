package my.example.domain

import my.example.domain.model.{MobAppClickstream, MobAppClickstreamCsv, Purchase}
import org.apache.spark.sql.functions.{col, from_json, regexp_replace}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

object LoadData {
  val pathMobAppClickstream: String = getClass.getResource("/mobile-app-clickstream_sample.csv").getPath
  val pathPurchase: String = getClass.getResource("/purchases_sample.csv").getPath

  private def multiReplace(clmn: Column, fromAndToValues: Seq[(String, String)]): Column = {
    fromAndToValues.foldLeft(clmn) {
      case (col, (from, to)) =>
        regexp_replace(col, from, to)
    }
  }

  def readMobAppClickstream(implicit spark: SparkSession): Dataset[MobAppClickstream] = {
    val mobAppClickstreamCsvEncoder = Encoders.product[MobAppClickstreamCsv]
    val mobAppClickstreamCsvSchema = mobAppClickstreamCsvEncoder.schema
    val mobAppClickstreamEncoder = Encoders.product[MobAppClickstream]

    val mobAppClickstream = spark
      .read
      .schema(mobAppClickstreamCsvSchema)
      .option("escape", "\"")
      .option("header", value = true)
      .csv(pathMobAppClickstream)
      .as[MobAppClickstreamCsv](mobAppClickstreamCsvEncoder)

    val jsonSchema = MapType(StringType, StringType, valueContainsNull = true)

    // fix doubling of opening and closing curly braces and abnormal quote character
    val attributesArtifactsFixes =  Seq(
      ("""^\{\{""", "{"),
      ("""}}$""", "}"),
      ("""“""", "\"")
    )

    mobAppClickstream
      .withColumn("attributes", multiReplace(col("attributes"), attributesArtifactsFixes))
      .withColumn("attributes", from_json(col("attributes"), jsonSchema))
      .as[MobAppClickstream](mobAppClickstreamEncoder)
  }

  def readPurchases(implicit spark: SparkSession): Dataset[Purchase] = {
    val purchaseEncoder = Encoders.product[Purchase]
    val purchaseSchema = purchaseEncoder.schema

    spark
      .read
      .schema(purchaseSchema)
      .option("header", value = true)
      .csv(pathPurchase)
      .as[Purchase](purchaseEncoder)
  }
}
