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

object Repartition_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.repartitionByRange(10.toInt,
                          col("c_timestamp").asc,
                          col("`c   short  --`").asc,
                          col("`c  date`").desc,
                          concat(col("c_timestamp"), col("`c  date`")).desc
    )

}
