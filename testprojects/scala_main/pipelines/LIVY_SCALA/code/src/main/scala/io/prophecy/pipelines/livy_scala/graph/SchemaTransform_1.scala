package io.prophecy.pipelines.livy_scala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import io.prophecy.pipelines.livy_scala.udfs.UDFs._
import io.prophecy.pipelines.livy_scala.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SchemaTransform_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.withColumn("indus_concat", expr(Config.c_st_expr))
      .drop("unit")
      .withColumnRenamed("value", "value_new")

}