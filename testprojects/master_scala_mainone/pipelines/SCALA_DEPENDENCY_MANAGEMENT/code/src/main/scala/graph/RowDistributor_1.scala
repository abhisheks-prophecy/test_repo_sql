package graph

import io.prophecy.libs._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RowDistributor_1 {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(expr(context.config.c_rd_expr)),
     in.filter(col("`c -  boolean _  `").isin(true, false))
    )

}
