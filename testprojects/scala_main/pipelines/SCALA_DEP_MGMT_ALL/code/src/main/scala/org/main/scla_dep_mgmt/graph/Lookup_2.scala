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

object Lookup_2 {

  def apply(spark: SparkSession, in0: DataFrame): Unit =
    createLookup("Lookup_2",
                 in0,
                 spark,
                 List("p_string", "p_long"),
                 "c_date-for today"
    )

}
