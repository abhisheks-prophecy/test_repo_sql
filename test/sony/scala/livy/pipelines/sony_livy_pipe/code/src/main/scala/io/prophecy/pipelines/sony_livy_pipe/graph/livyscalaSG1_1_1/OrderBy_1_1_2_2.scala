package io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_1

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_1_1_2_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(
      col("year").asc,
      col("industry_code_ANZSIC").asc,
      col("industry_name_ANZSIC").asc,
      col("rme_size_grp").asc,
      col("variable").asc,
      col("value").asc,
      col("unit").asc
    )

}