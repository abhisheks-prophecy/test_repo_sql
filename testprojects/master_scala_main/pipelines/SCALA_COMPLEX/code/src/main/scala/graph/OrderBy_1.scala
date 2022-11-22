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

object OrderBy_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.orderBy(
      col("c_timestamp").asc,
      concat(col("`c  date`"), col("c_timestamp")).desc,
      col("`c-int-column type`").asc,
      col("`-- c-long`").asc,
      concat(col("`c  date`"), col("c_timestamp")).desc,
      concat(col("`c  date`"), col("c_timestamp")).desc,
      col("`c   short  --`").asc,
      col("`c-int-column type`").asc,
      col("`-- c-long`").asc,
      col("`c-decimal`").asc,
      col("`c  float`").asc,
      col("`c--boolean`").asc,
      col("`c- - -double`").asc,
      col("`c___-- string`").asc,
      col("`c  date`").asc,
      col("c_timestamp").asc
    )

}