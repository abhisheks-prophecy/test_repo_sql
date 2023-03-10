package graph.everythingSG_1

import io.prophecy.libs._
import udfs.UDFs._
import graph.everythingSG_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SetOperation_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0.unionAll(in1)

}
