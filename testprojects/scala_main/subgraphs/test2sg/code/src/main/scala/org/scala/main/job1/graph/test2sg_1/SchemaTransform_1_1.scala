package org.scala.main.job1.graph.test2sg_1

import io.prophecy.libs._
import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.udfs.UDFs._
import com.scala.main.job1.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object SchemaTransform_1_1 {
  def apply(spark: SparkSession, in: DataFrame): DataFrame = in
}