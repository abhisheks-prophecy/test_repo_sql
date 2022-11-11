package graph

import io.prophecy.libs._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_1 {
  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame, in4: DataFrame, in5: DataFrame): DataFrame = {
    var result=Config.c_0*Config.c_1
    assert(result==0)
    var out0=in0.filter(col("customer_id")  > 5)
    var out1=in1.filter(col("customer_id")  > 5)
    var out2=in2.filter(col("first_name") === "%A%" )
    var out3=in3.distinct()
    var out4=in4.filter(col("customer_id")  > 5)
    var out5=in5.filter(col("customer_id")  > 5)
    out0=out1.union(out2).union(out3).union(out4).union(out5)
    out0
  }

}
