package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
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
        "c_date-for today",
        expr(Config.c_row).over(
          Window
            .partitionBy(lit(Config.c_short),
                         col("`c  - int`"),
                         col("`- c long`")
            )
            .orderBy(col("`c_float-__  `").asc,
                     lit(Config.c_bool).desc,
                     col("c_double").asc,
                     col("`c-string`").asc
            )
        )
      )
      .withColumn(
        "c_timestamp  __ for--today",
        row_number().over(
          Window
            .partitionBy(lit(Config.c_short),
                         col("`c  - int`"),
                         col("`- c long`")
            )
            .orderBy(col("`c_float-__  `").asc,
                     lit(Config.c_bool).desc,
                     col("c_double").asc,
                     col("`c-string`").asc
            )
        )
      )
  }

}
