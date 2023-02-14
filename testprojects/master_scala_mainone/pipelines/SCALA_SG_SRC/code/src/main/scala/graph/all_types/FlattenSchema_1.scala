package graph.all_types

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

object FlattenSchema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("c_array--long", explode_outer(col("c_array--long")))
      .withColumn("c_array -- decimal",
                  explode_outer(col("c_array -- decimal"))
      )
      .withColumn("c_array-int  _ int",
                  explode_outer(col("c_array-int  _ int"))
      )
      .withColumn(
        "c_struct -- _  -c_array_int - of a struct ",
        explode_outer(col("c_struct -- _  .c_array_int - of a struct "))
      )
      .select(
        col("c_array--long"),
        col("c_array -- decimal"),
        col("c_struct -- _  .c_array_int - of a struct ")
          .as("c_struct -- _  -c_array_int - of a struct "),
        col("c_struct -- _  .c_timestamp - of a struct-")
          .as("c_struct -- _  -c_timestamp - of a struct-"),
        col("c_struct -- _  .c_string - of a struct -- _")
          .as("c_struct -- _  -c_string - of a struct -- _"),
        col("c_array-int  _ int")
      )

}
