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

object Aggregate_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("prediction"))
      .agg(
        first(col("Y_Value_of_Occupied_Homes")).as("Y_Value_of_Occupied_Homes"),
        first(col("Crime_Rate")).as("Crime_Rate"),
        first(col("Residential_Land_Zone")).as("Residential_Land_Zone"),
        first(col("Non_retail_Business_acres")).as("Non_retail_Business_acres"),
        first(col("Charles_River")).as("Charles_River"),
        first(col("Nitric_Oxide")).as("Nitric_Oxide"),
        first(col("Average_Rooms")).as("Average_Rooms"),
        first(col("Owner_Occupied_Units")).as("Owner_Occupied_Units"),
        first(col("Distance_to_Employment_Centers"))
          .as("Distance_to_Employment_Centers"),
        first(col("Accessibility_to_Highways")).as("Accessibility_to_Highways"),
        first(col("Property_Tax_Rate")).as("Property_Tax_Rate"),
        first(col("Pupil_Teacher_Ratio")).as("Pupil_Teacher_Ratio"),
        first(col("Lower_Status")).as("Lower_Status"),
        first(col("features")).as("features"),
        first(col("prediction")).as("prediction")
      )

}
