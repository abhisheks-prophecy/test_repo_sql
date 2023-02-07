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

object Filter_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("`c-string`")
        .like("%3%")
        .or(col("`c-string`").like(context.config.c_regex_filter))
    )

}
