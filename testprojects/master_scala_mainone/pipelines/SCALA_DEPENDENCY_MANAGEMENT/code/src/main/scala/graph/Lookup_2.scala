package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_2 {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("Lookup_2",
                 in0,
                 context.spark,
                 List("customer_id", "first_name"),
                 "email",
                 "country_code"
    )

}
