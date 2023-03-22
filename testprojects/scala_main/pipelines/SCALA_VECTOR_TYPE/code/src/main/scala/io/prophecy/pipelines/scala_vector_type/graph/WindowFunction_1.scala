package io.prophecy.pipelines.scala_vector_type.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object WindowFunction_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn("Average_Rooms",
                  row_number().over(
                    Window
                      .partitionBy(col("Y_Value_of_Occupied_Homes"))
                      .orderBy(col("Crime_Rate").asc)
                  )
    )
  }

}
