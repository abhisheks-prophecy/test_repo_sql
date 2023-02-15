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

object Deduplicate_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(
          Window
            .partitionBy("c -  boolean _  ", "c-string")
            .orderBy(
              concat(lit(Config.c_repartition_colname), col("`c  - int`")).asc,
              lit(Config.c_expr_schematransform).desc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
