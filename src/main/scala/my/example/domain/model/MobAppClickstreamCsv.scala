package my.example.domain.model

import java.sql.Timestamp

case class MobAppClickstreamCsv(
                              userId: String,
                              eventId: String,
                              eventTime: Timestamp,
                              eventType: String,
                              attributes: String
                            )
