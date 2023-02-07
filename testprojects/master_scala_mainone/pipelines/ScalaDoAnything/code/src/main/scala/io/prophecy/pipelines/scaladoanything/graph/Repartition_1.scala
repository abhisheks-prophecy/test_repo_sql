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

object Repartition_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.repartition(10.toInt,
                   concat(lit(Config.c_repartition_colname),
                          col("`c  float`"),
                          col("`c   short  --`")
                   ),
                   expr(Config.c_repartition_expr)
    )
  }

}
