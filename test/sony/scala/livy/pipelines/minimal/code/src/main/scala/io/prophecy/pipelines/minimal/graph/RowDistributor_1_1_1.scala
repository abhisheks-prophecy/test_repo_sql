package io.prophecy.pipelines.minimal.graph

import io.prophecy.libs._
import io.prophecy.pipelines.minimal.config.ConfigStore._
import io.prophecy.pipelines.minimal.config.Context
import io.prophecy.pipelines.minimal.udfs.UDFs._
import io.prophecy.pipelines.minimal.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RowDistributor_1_1_1 {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("year").like(context.config.c_expr)),
     in.filter(col("rme_size_grp").like("%b%"))
    )

}
