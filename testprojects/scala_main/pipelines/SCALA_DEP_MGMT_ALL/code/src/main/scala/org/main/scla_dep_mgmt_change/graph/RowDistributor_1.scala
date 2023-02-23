package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.ConfigStore._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RowDistributor_1 {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(col("`c_decimal  -  `") >= lit(Config.c_12321)),
     in.filter(expr(Config.c_rd_expr))
    )
  }

}
