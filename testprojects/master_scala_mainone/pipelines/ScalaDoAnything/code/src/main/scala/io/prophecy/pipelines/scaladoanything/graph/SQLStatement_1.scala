package io.prophecy.pipelines.scaladoanything.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scaladoanything.config.ConfigStore._
import io.prophecy.pipelines.scaladoanything.udfs.UDFs._
import io.prophecy.pipelines.scaladoanything.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SQLStatement_1 {

  def apply(spark: SparkSession, in0: DataFrame): (DataFrame, DataFrame) = {
    in0.createOrReplaceTempView("in0")
    (spark.sql(
       "select * from in0 where in0.`c  - int` > 0 and in0.`c- short` > 1"
     ),
     spark.sql(
       "select * from in0 where in0.`c  - int` != '$c_int' and in0.`c-string` not like '$c_sql_pattern'"
     )
    )
  }

}
