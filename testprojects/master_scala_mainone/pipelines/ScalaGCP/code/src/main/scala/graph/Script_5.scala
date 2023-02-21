package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_5 {
  def apply(context: Context, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    var out1=in0.select(col("customer_id")).withColumnRenamed("customer_id", "c_new_str")
    var out2=in1.select(col("c_struct -- _  -c_string - of a struct -- _")).withColumnRenamed("c_struct -- _  -c_string - of a struct -- _", "c_new_str")
    var out3=in2.select(col("c-string")).withColumnRenamed("c-string", "c_new_str")
    var out4=in3.withColumn("c  - int", col("c  - int").cast(StringType)).select(col("c  - int")).withColumnRenamed("c  - int", "c_new_str")
    var out0=out1.union(out2).union(out3).union(out4)
    out0
  }

}
