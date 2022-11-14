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

object FlattenSchema_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.withColumn("c_array-- boolean ",
                  explode_outer(col("c_array-- boolean "))
      )
      .withColumn("-- c_array_timestamp -- ",
                  explode_outer(col("-- c_array_timestamp -- "))
      )
      .withColumn("c_array-string  _ string",
                  explode_outer(col("c_array-string  _ string"))
      )
      .withColumn("c_array-int  _ int",
                  explode_outer(col("c_array-int  _ int"))
      )
      .withColumn("c_array -- float", explode_outer(col("c_array -- float")))
      .withColumn("c_array -- decimal",
                  explode_outer(col("c_array -- decimal"))
      )
      .withColumn(
        "c_struct -- _  -c_array_int - of a struct ",
        explode_outer(col("c_struct -- _  .c_array_int - of a struct "))
      )
      .select(
        col("c_array-- boolean "),
        col("-- c_array_timestamp -- "),
        col("c_array-string  _ string"),
        col("c_array-int  _ int"),
        col("c_array -- float"),
        col("c_array -- decimal"),
        col("c_struct -- _  .c_array_int - of a struct ")
          .as("c_struct -- _  -c_array_int - of a struct "),
        col("p_decimal"),
        col("p_float")
      )

}
