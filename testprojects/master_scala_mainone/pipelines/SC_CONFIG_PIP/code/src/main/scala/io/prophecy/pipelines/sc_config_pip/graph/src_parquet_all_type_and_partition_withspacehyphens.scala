package io.prophecy.pipelines.sc_config_pip.graph

import io.prophecy.libs._
import io.prophecy.pipelines.sc_config_pip.config.ConfigStore._
import io.prophecy.pipelines.sc_config_pip.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_parquet_all_type_and_partition_withspacehyphens {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load(
        "dbfs:/Prophecy/qa_data/parquet/all_type_and_partition_withspacehyphens"
      )

}
