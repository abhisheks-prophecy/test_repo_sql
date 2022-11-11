package graph

import io.prophecy.libs._
import config.ConfigStore._
import config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object SubGraph_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_9 = Reformat_9(spark, in0).interim(
      "SubGraph_1",
      "1cQA3IfXw39MFd4p4KfFz$$Mh1Cs5PftltLu_aHYYdgo",
      "7PlOCNRfqR9BI6pdIHB8P$$msMi87LZqpk37MvJ9o7te"
    )
    df_Reformat_9
  }

}
