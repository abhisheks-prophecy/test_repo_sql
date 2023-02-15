package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object WindowFunction_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
        "c_date-for today",
        expr(context.config.c_row).over(
          Window
            .partitionBy(col("`c- short`"),
                         col("`c  - int`"),
                         col("`- c long`")
            )
            .orderBy(col("`c_float-__  `").asc,
                     col("`c -  boolean _  `").asc,
                     col("c_double").asc,
                     col("`c-string`").asc
            )
        )
      )
      .withColumn(
        "c_timestamp  __ for--today",
        row_number().over(
          Window
            .partitionBy(col("`c- short`"),
                         col("`c  - int`"),
                         col("`- c long`")
            )
            .orderBy(col("`c_float-__  `").asc,
                     col("`c -  boolean _  `").asc,
                     col("c_double").asc,
                     col("`c-string`").asc
            )
        )
      )
  }

}
