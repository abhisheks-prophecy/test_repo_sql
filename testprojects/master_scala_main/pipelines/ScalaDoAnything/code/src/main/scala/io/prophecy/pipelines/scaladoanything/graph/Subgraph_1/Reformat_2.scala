package io.prophecy.pipelines.scaladoanything.graph.Subgraph_1

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

object Reformat_2 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      concat(
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
      ).as("c_config"),
      col("`c   short  --`").as("c   short  --"),
      col("`c-int-column type`").as("c-int-column type"),
      col("`-- c-long`").as("-- c-long"),
      col("`c-decimal`").as("c-decimal"),
      col("`c  float`").as("c  float"),
      col("`c--boolean`").as("c--boolean"),
      col("`c- - -double`").as("c- - -double"),
      col("`c___-- string`").as("c___-- string"),
      col("`c  date`").as("c  date"),
      col("c_timestamp"),
      col("c_new_col")
    )

}
