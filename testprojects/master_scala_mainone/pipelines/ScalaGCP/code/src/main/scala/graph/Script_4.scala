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

object Script_4 {
  def apply(spark: SparkSession, in0: DataFrame): Unit = {
    var out=in0.filter(col("c_short")  > -1)
    print(out.show())
  }

}
