package io.prophecy.pipelines.scaladoanything.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scaladoanything.config.ConfigStore._
import io.prophecy.pipelines.scaladoanything.config.Context
import io.prophecy.pipelines.scaladoanything.udfs.UDFs._
import io.prophecy.pipelines.scaladoanything.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(
      col("c_double").asc,
      col("`c-string`").desc,
      concat(lit(context.config.c_repartition_colname), col("`c  - int`")).asc
    )

}
