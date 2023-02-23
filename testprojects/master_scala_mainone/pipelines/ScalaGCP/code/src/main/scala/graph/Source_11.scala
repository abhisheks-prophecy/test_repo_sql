package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_11 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("delta")
      .load("dbfs:/Prophecy/qa_data/delta/all_type_no_partition")

}
