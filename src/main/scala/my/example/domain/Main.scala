package my.example.domain

import my.example.domain.model.{MobAppClickstream, Purchase}
import org.apache.spark.sql.{DataFrame, Dataset}

object Main extends App with SparkTestApp {
  override def main(args: Array[String] ): Unit = {

  val mobAppClickstream: Dataset[MobAppClickstream] = LoadData.readMobAppClickstream.cache
  val purchases: Dataset[Purchase] = LoadData.readPurchases.cache

  val task1part1: DataFrame = Task1Part1PurchasesAttribution.calculate(mobAppClickstream, purchases).cache
  val task1part2: DataFrame = Task1Part2PurchasesAttribution.calculate(mobAppClickstream, purchases).cache

  //"A file should be read correctly"
    mobAppClickstream
      .show(50, truncate = false)

    purchases
      .show(50, truncate = false)

  //"Task #1.1 should build purchases attribution projection with Spark SQL"
    task1part1
      .show(50, truncate = false)

  //"Task #1.2 should build purchases attribution projection using UDAF"
    task1part2
      .show(50, truncate = false)

  //"Task #2.1 should show top 10 marketing campaigns with the biggest revenue"
    task1part1
      .createOrReplaceTempView("purchases_projection")

    spark.sql(
      """SELECT sum(billingCost) revenue, campaignId
        |FROM (
        |  SELECT distinct campaignId, purchaseId, billingCost
        |  FROM purchases_projection
        |  WHERE isConfirmed
        |) confirmed_billings
        |GROUP BY campaignId
        |ORDER BY sum(billingCost) desc
        |LIMIT 10""".stripMargin)
      .show(50, truncate = false)

  //Task #2.2 should show the most popular channel in each campaign by unique sessions"
    task1part1
      .createOrReplaceTempView("purchases_projection")

    spark.sql(
      """SELECT campaignId, channelId
        |FROM (
        |  SELECT rank(sessionsCount.cnt) OVER (PARTITION BY campaignId ORDER BY sessionsCount.cnt) rnk, campaignId, channelId
        |  FROM (
        |    SELECT campaignId, channelId, count(distinct sessionId) cnt
        |    FROM purchases_projection
        |    GROUP BY campaignId, channelId
        |  ) sessionsCount
        |) withRank
        |WHERE rnk = 1
        |""".stripMargin)
      .show(50, truncate = false)

  }
}
