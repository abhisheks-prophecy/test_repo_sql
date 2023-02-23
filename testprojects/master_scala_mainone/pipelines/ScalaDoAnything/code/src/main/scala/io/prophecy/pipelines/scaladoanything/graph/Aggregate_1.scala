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

object Aggregate_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.groupBy(col("`c -  boolean _  `"),
               lit(Config.c_date_for_today).as("`c_date-for today`")
      )
      .pivot(col("`c- short`"),
             List("`c_timestamp  __ for--today`", "p_int", "'$c_float_name'")
      )
      .agg(expr(Config.c_agg_expr).as("c- short"),
           first(col("`c  - int`")).as("c  - int")
      )
  }

}
