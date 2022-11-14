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

object SQLStatement_1 {

  def apply(spark: SparkSession, in0: DataFrame): (DataFrame, DataFrame) = {
    in0.createOrReplaceTempView("in0")
    (spark.sql(
       "select * from in0 where in0.customer_id not like '$c_sql_expr'"
     ),
     spark.sql("select * from in0 where in0.first_name not like '$c_sql_expr'")
    )
  }

}
