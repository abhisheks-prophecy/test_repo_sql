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

object Join_1 {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"), expr(Config.c_join_condition), "inner")
      .where(col("in0.`c  float`") < lit(Config.c_int))
      .select(
        col("in0.`c   short  --`").as("c   short  --"),
        col("in0.`c-int-column type`").as("c-int-column type"),
        col("in0.`-- c-long`").as("-- c-long"),
        col("in0.`c-decimal`").as("c-decimal"),
        col("in0.`c  float`").as("c  float"),
        col("in0.`c--boolean`").as("c--boolean"),
        col("in0.`c- - -double`").as("c- - -double"),
        col("in0.`c___-- string`").as("c___-- string"),
        col("in0.`c  date`").as("c  date"),
        col("in0.c_timestamp").as("c_timestamp"),
        concat(
          col("in0.`c  float`"),
          lit(Config.c_boolean),
          lit(" :: "),
          lit(Config.c_double),
          lit(" :: "),
          lit(Config.c_float),
          lit(" :: "),
          lit(Config.c_int),
          lit(" :: "),
          lit(Config.c_long),
          lit(" :: "),
          lit(Config.c_short),
          lit(" :: "),
          lit(Config.c_string1)
        ).as("c_new_col")
      )

}
