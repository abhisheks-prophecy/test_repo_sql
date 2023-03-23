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

object Join_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.Average_Rooms") === col("in1.Average_Rooms"),
            "inner"
      )
      .select(
        col("in0.Y_Value_of_Occupied_Homes").as("Y_Value_of_Occupied_Homes"),
        col("in0.Crime_Rate").as("Crime_Rate"),
        col("in0.Residential_Land_Zone").as("Residential_Land_Zone"),
        col("in0.Non_retail_Business_acres").as("Non_retail_Business_acres"),
        col("in0.Charles_River").as("Charles_River"),
        col("in0.Nitric_Oxide").as("Nitric_Oxide"),
        col("in0.Average_Rooms").as("Average_Rooms"),
        col("in0.Owner_Occupied_Units").as("Owner_Occupied_Units"),
        col("in0.Distance_to_Employment_Centers")
          .as("Distance_to_Employment_Centers"),
        col("in0.Accessibility_to_Highways").as("Accessibility_to_Highways"),
        col("in0.Property_Tax_Rate").as("Property_Tax_Rate"),
        col("in0.Pupil_Teacher_Ratio").as("Pupil_Teacher_Ratio"),
        col("in0.Lower_Status").as("Lower_Status"),
        col("in1.features").as("features"),
        col("in0.prediction").as("prediction")
      )

}
