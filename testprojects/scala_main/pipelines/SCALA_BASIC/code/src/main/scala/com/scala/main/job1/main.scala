package com.scala.main.job1

import io.prophecy.libs._
import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.config.Context
import com.scala.main.job1.config._
import com.scala.main.job1.udfs.UDFs._
import com.scala.main.job1.udfs._
import com.scala.main.job1.graph._
import com.scala.main.job1.graph.SubgraphMain
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens1 =
      src_parquet_all_type_and_partition_withspacehyphens1(context).interim(
        "graph",
        "o8K-lNobc6Z8Asi3dRegs$$Buw8lxPhFtSUcFZhGxXbx",
        "Yui765QKx0wOKHaOnjtzk$$Q04y-nxgyv0ZfORhocEUn"
      )
    val df_SchemaTransform_1 =
      SchemaTransform_1(context,
                        df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "5IEpMUJQMpUIx6Hv3eZVS$$9f4baBrU_1q1LbFl9fY2n",
                "vx64sYjC4vVrmBOSkCOXa$$qpmxU6WcJBG1bBJ5VrLY-"
      )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()
    val df_SCALA_BASIC12 =
      SCALA_BASIC12(context,
                    df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "qS7udi_fVyLwNFW-Mm0CD$$E1cMJlFkiBy9SppQZb6w0",
                "1vob0WwUeTByi5ggao1MM$$RLb5byOWJfwhdby-ImTGc"
      )
    df_SCALA_BASIC12.cache().count()
    df_SCALA_BASIC12.unpersist()
    val df_SubgraphMain = SubgraphMain.apply(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens1
    )
    val df_Reformat_1 = Reformat_1(context, df_SubgraphMain).interim(
      "graph",
      "9SxukrkbLjB9767nnjjyc$$HKrQPtYnAcfBjk1YLp12B",
      "q2OHFdKSlaApfiB-p2kod$$gjcVI8oVJu6r8jvarb7gm"
    )
    val df_SCALA_BASIC1 =
      SCALA_BASIC1(context,
                   df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "bl47XMiEa-WNOlMEK4sFp$$2Dwp_ulOdXsBz8xjpntdm",
                "KHx3a7pEn7ZIJRnqGorWt$$RNdicUvC7gj0La9ZxSfE2"
      )
    df_SCALA_BASIC1.cache().count()
    df_SCALA_BASIC1.unpersist()
    val df_Reformat_4 = Reformat_4(context, df_Reformat_1).interim(
      "graph",
      "Jsldsl3d5xD4SRjpdKI-Z$$DuCm45gqMbljkiKqBMQjw",
      "sPVo9omm0xwOufqyXCGI8$$F12AVA_FrP1_w296nytQb"
    )
    df_Reformat_4.cache().count()
    df_Reformat_4.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("spark_config1",                  "spark_config_value_1")
    spark.conf.set("spark_config2",                  "spark_config_value_2")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_BASIC")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",      "hadoop_config_value2")
    MetricsCollector.start(spark, "pipelines/SCALA_BASIC")
    graph(context)
    MetricsCollector.end(spark)
  }

}
