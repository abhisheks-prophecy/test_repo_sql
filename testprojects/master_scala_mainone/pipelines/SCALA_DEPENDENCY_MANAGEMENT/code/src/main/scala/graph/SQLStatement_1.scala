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

object SQLStatement_1 {

  def apply(context: Context, in0: DataFrame): (DataFrame, DataFrame) = {
    in0.createOrReplaceTempView("in0")
    (context.spark.sql(
       "select * from in0 where in0.customer_id not like '$c_sql_expr'"
     ),
     context.spark.sql(
       "select * from in0 where in0.first_name not like '$c_sql_expr'"
     )
    )
  }

}
