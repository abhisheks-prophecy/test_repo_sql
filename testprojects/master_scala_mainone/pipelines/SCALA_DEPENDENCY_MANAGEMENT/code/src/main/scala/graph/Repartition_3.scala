package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Repartition_3 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.repartition(concat(col("`c- short`"), col("`c  - int`")))

}
