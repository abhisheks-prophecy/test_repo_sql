package com.main.sub_graph_src1.graph.Subgraph_1

import io.prophecy.libs._
import com.main.sub_graph_src1.config.ConfigStore._
import com.main.sub_graph_src1.config.Context
import com.main.sub_graph_src1.udfs.UDFs._
import com.main.sub_graph_src1.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_1_1 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    var out0=in0
    out0
  }

}
