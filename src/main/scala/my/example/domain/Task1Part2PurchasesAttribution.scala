package my.example.domain

import my.example.domain.model.{MobAppClickstream, Purchase}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Task1Part2PurchasesAttribution {
  def calculate(mobAppClickstream: Dataset[MobAppClickstream], purchases: Dataset[Purchase])
               (implicit spark: SparkSession): DataFrame = {
    spark.udf.register("last_non_empty", new LastNonEmpty)

    mobAppClickstream
      .createOrReplaceTempView("mob_app_clickstream")

    purchases
      .createOrReplaceTempView("purchases")

    spark.sql(
      """
        |WITH clickstream AS (
        |  SELECT *, attributes["campaign_id"] campaignId, attributes["channel_id"] channelId, attributes["purchase_id"] purchaseId
        |  FROM mob_app_clickstream
        | )
        |
        |,rn_app_open_event AS (
        |  SELECT eventId, row_number() OVER (PARTITION BY userId, eventType ORDER BY eventTime) app_open_rn
        |  FROM clickstream
        |  WHERE eventType = 'app_open'
        |)
        |
        |-- mark all events with the last app_open_rn (before current event)
        |,sessions_marked AS (
        |  SELECT clickstream.*
        |  ,last_non_empty(rn_app_open_event.app_open_rn) OVER (PARTITION BY userId ORDER BY eventTime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sessionRn
        |  FROM clickstream
        |  LEFT JOIN rn_app_open_event
        |  ON clickstream.eventId = rn_app_open_event.eventId
        |)
        |
        |,clickstream_sessions AS (
        |  SELECT sessions_marked.*, concat(userId, '_', sessionRn) sessionId
        |  FROM sessions_marked
        |)
        |
        |,clickstream_complete AS (
        |SELECT clickstream_sessions.userId, clickstream_sessions.eventId, clickstream_sessions.eventTime, clickstream_sessions.eventType,
        |  clickstream_sessions.attributes, clickstream_sessions.sessionId, prch.purchaseId, cmpg_chnl.campaignId, cmpg_chnl.channelId
        |  FROM clickstream_sessions
        |  LEFT JOIN (SELECT sessionId, purchaseId FROM clickstream_sessions WHERE eventType = 'purchase') prch
        |  ON clickstream_sessions.sessionId = prch.sessionId
        |  LEFT JOIN (SELECT sessionId, campaignId, channelId FROM clickstream_sessions WHERE eventType = 'app_open') cmpg_chnl
        |  ON clickstream_sessions.sessionId = cmpg_chnl.sessionId
        |)
        |
        |SELECT purchases.*, clickstream_complete.sessionId, clickstream_complete.campaignId, clickstream_complete.channelId
        |FROM purchases
        |LEFT JOIN clickstream_complete
        |ON purchases.purchaseId = clickstream_complete.purchaseId
        | ORDER BY userId, eventTime
        |""".stripMargin)
  }
}