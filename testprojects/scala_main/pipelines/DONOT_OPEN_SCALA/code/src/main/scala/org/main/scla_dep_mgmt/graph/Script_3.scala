package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Script_3 {
  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame, in3: DataFrame): DataFrame = {
    var out1=in0.filter(col("c   short  --")  > 2).select(col("c   short  --"))
    var out2=in1.filter(col("c   short  --")  > 1).select(col("c   short  --"))
    var out4=in3.filter(col("c   short  --") > 3).select(col("c   short  --"))
    var out0=out1.union(out2).union(out4)
    out0
  }

}
