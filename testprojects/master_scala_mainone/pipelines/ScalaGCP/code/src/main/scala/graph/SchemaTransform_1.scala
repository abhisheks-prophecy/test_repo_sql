package graph

import io.prophecy.libs._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object SchemaTransform_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.withColumn("c_concat_new_short_decimal",
                  concat(col("`c_struct-c_short`"), col("`c_struct-c_decimal`"))
      )
      .drop("c_array_long")
      .withColumnRenamed("c_array_boolean", "c_array_boolean_renamed")

}
