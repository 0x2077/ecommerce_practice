package my.example.domain.model

import java.sql.Timestamp

case class Purchase(
                     purchaseId: String,
                     purchaseTime: Timestamp,
                     billingCost: Double,
                     isConfirmed: Boolean
                   )
