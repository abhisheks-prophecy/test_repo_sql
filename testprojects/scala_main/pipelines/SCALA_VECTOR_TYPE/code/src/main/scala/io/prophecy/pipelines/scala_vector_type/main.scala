package io.prophecy.pipelines.scala_vector_type

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import io.prophecy.pipelines.scala_vector_type.config._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.udfs._
import io.prophecy.pipelines.scala_vector_type.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_normalization_csv_dataset = normalization_csv_dataset(context)
    val df_Script_1                  = Script_1(context,   df_normalization_csv_dataset)
    val df_Reformat_1                = Reformat_1(context, df_Script_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_VECTOR_TYPE")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/SCALA_VECTOR_TYPE")
    apply(context)
    MetricsCollector.end(spark)
  }

}
