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

object UTGenReformat3 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("`c   short  --`").as("c   short  --"),
              col("`c   short  --`").as("c_next")
    )

}
