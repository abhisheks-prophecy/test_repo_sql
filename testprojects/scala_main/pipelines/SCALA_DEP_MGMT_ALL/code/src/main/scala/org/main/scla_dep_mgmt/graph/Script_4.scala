package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_4 {
  def apply(spark: SparkSession, in0: DataFrame): Unit = {
    var out=in0 //.filter(col("c_short")  > -654566)
    print(out.show())
    println(Config.CONFIG_STR)
    println(Config.CONFIG_BOOLEAN)
    println(Config.CONFIG_DOUBLE)
    println(Config.CONFIG_INT)
    println(Config.CONFIG_FLOAT)
    println(Config.c_agg_expr)
  }

}
