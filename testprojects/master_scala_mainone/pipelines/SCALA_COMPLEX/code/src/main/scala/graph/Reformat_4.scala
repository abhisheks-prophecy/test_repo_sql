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

object Reformat_4 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      lookup("Lookup_2", col("p_string"), col("p_long"))
        .getField("c_date-for today")
        .as("col1")
    )

}
