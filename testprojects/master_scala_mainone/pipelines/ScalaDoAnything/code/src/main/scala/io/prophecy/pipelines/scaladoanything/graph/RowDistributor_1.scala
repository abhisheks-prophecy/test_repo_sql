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

object RowDistributor_1 {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(
       (col("`c  float`") < lit(5))
         .and(col("`c-int-column type`") > lit(Config.c_85))
     ),
     in.filter(expr(Config.c_rd_expr))
    )
  }

}
