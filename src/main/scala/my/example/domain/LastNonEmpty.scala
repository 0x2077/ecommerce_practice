package my.example.domain

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class LastNonEmpty extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(List(StructField("value", StringType)))

  // schema of the row which used for aggregation
  override def bufferSchema: StructType = StructType(
    List(StructField("keep_value", StringType))
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  // initial value in each group
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // called for each input record of the group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newValue = input.getAs[String](0)
    buffer(0) = {
      if (newValue != null && newValue != "") newValue
      else buffer(0)
    }
  }

  // if function supports partial aggregates, spark might (as an optimization) compute partial results and combine them together
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // FIXME: non-additive function: merge should be prohibited
    val newValue = buffer2.getAs[String](0)
    buffer1(0) = {
      if (newValue != null && newValue != "") newValue
      else buffer1(0)
    }
  }

  // called after all entries for a group exhausted
  override def evaluate(buffer: Row): Any = buffer.getAs[String](0)
}
