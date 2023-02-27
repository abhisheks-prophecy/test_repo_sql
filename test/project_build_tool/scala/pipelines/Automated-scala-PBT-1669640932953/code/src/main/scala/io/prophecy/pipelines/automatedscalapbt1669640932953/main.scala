package io.prophecy.pipelines.automatedscalapbt1669640932953

import io.prophecy.libs._
import io.prophecy.pipelines.automatedscalapbt1669640932953.config.ConfigStore._
import io.prophecy.pipelines.automatedscalapbt1669640932953.config._
import io.prophecy.pipelines.automatedscalapbt1669640932953.udfs.UDFs._
import io.prophecy.pipelines.automatedscalapbt1669640932953.udfs._
import io.prophecy.pipelines.automatedscalapbt1669640932953.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_dataset_cust_in = dataset_cust_in(spark)
    val df_Reformat_1      = Reformat_1(spark, df_dataset_cust_in)
  }

  def main(args: Array[String]): Unit = {
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/Automated-scala-PBT-1669640932953"
    )
    MetricsCollector.start(
      spark,
      spark.conf.get(
        "prophecy.project.id"
      ) + "/" + "pipelines/Automated-scala-PBT-1669640932953"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
