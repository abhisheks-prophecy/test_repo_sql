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

object SQLStatement_1 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): (DataFrame, DataFrame) = {
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    in2.createOrReplaceTempView("in2")
    (context.spark.sql(
       "select in0.col1,in1.col2,in2.c8_c1,in0.c8_c2 from in0,in1,in2 where in0.c8_c2 not like '$c_sql_expr'"
     ),
     context.spark.sql(
       "select * from in0 where cast(SUBSTRING(in0.c9_udf1_c2, 1,2) as int) > -1"
     )
    )
  }

}
