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

object RowDistributor_1 {

  def apply(spark: SparkSession, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("`c_decimal  -  `") >= lit(12321)),
     in.filter(col("`c -  boolean _  `").isin(true, false))
    )

}
