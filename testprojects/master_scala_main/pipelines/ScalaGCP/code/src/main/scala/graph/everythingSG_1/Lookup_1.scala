package graph.everythingSG_1

import io.prophecy.libs._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Lookup_1 {

  def apply(spark: SparkSession, in0: DataFrame): Unit =
    createLookup("Lookup_1",
                 in0,
                 spark,
                 List("customer_id", "first_name"),
                 "last_name",
                 "phone"
    )

}
