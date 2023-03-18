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

object Filter_3 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      !col("first_name")
        .like(Config.c_regex1)
        .and(!col("country_code").like(Config.c_regex2))
    )
  }

}
