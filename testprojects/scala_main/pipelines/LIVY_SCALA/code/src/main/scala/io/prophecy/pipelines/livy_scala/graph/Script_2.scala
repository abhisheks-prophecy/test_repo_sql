package io.prophecy.pipelines.livy_scala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import io.prophecy.pipelines.livy_scala.config.Context
import io.prophecy.pipelines.livy_scala.udfs.UDFs._
import io.prophecy.pipelines.livy_scala.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_2 {
  def apply(context: Context, in0: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    var out0=in0
    var out1=in0.filter(col("year")  >= 2010)
    out1.show()
  }

}
