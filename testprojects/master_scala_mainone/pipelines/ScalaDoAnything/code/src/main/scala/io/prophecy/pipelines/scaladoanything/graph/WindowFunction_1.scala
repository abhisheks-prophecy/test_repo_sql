package io.prophecy.pipelines.scaladoanything.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scaladoanything.config.ConfigStore._
import io.prophecy.pipelines.scaladoanything.udfs.UDFs._
import io.prophecy.pipelines.scaladoanything.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object WindowFunction_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
        "c_decimal  -  ",
        expr(Config.c_row_number).over(
          Window
            .partitionBy(lit(Config.c_int_name), col("`c- short`"))
            .orderBy(col("`c_float-__  `").asc,
                     col("`c_decimal  -  `").asc,
                     lit(Config.c_repartition_expr).desc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .withColumn(
        "c_date-for today",
        row_number().over(
          Window
            .partitionBy(lit(Config.c_int_name), col("`c- short`"))
            .orderBy(col("`c_float-__  `").asc,
                     col("`c_decimal  -  `").asc,
                     lit(Config.c_repartition_expr).desc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .withColumn(
        "p_double",
        expr(Config.c_row_number).over(
          Window
            .partitionBy(lit(Config.c_int_name), col("`c- short`"))
            .orderBy(col("`c_float-__  `").asc,
                     col("`c_decimal  -  `").asc,
                     lit(Config.c_repartition_expr).desc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .withColumn(
        "p_string",
        row_number().over(
          Window
            .partitionBy(lit(Config.c_int_name), col("`c- short`"))
            .orderBy(col("`c_float-__  `").asc,
                     col("`c_decimal  -  `").asc,
                     lit(Config.c_repartition_expr).desc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
  }

}
