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

object RowDistributor_1 {

  def apply(spark: SparkSession, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(expr(Config.c_rd_expr)),
     in.filter(col("`c -  boolean _  `").isin(true, false))
    )

}
