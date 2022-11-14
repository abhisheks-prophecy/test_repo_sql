package io.prophecy.pipelines.testpipeline.graph.everythingSG_1

import io.prophecy.libs._
import io.prophecy.pipelines.testpipeline.config.ConfigStore._
import io.prophecy.pipelines.testpipeline.udfs.UDFs._
import io.prophecy.pipelines.testpipeline.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_1 {

  def apply(spark: SparkSession, in0: DataFrame): Unit =
    createLookup("Lookup_1",
                 in0,
                 spark,
                 List("customer_id", "first_name"),
                 "last_name",
                 "phone"
    )

}
