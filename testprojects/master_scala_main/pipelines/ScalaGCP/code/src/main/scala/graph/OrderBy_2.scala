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

object OrderBy_2 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.orderBy(
      col("customer_id").asc,
      col("first_name").asc,
      col("last_name").asc,
      col("phone").asc,
      col("email").asc,
      col("country_code").asc,
      col("account_open_date").asc,
      col("account_flags").asc,
      col("config_values").desc
    )

}
