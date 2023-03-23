package io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("Y_Value_of_Occupied_Homes"),
      col("Crime_Rate"),
      col("Residential_Land_Zone"),
      col("Non_retail_Business_acres"),
      col("Charles_River"),
      col("Nitric_Oxide"),
      col("Average_Rooms"),
      col("Owner_Occupied_Units"),
      col("Distance_to_Employment_Centers"),
      col("Accessibility_to_Highways"),
      col("Property_Tax_Rate"),
      col("Pupil_Teacher_Ratio"),
      col("Lower_Status"),
      col("features"),
      col("prediction")
    )

}
