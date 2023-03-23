package com.scala.main.job1

import io.prophecy.libs._
import com.scala.main.job1.config.Context
import com.scala.main.job1.config._
import com.scala.main.job1.config.ConfigStore.interimOutput
import com.scala.main.job1.udfs.UDFs._
import com.scala.main.job1.udfs._
import com.scala.main.job1.graph._
import com.scala.main.job1.graph.SubgraphMain
import com.scala.main.job1.graph.Subgraph_1
import com.scala.main.job1.graph.Subgraph_3
import com.scala.main.job1.graph.Subgraph_4
import com.scala.main.job1.graph.SubgraphMain.config.{
  Context => SubgraphMain_Context
}
import com.scala.main.job1.graph.Subgraph_1.config.{
  Context => Subgraph_1_Context
}
import com.scala.main.job1.graph.Subgraph_3.config.{
  Context => Subgraph_3_Context
}
import com.scala.main.job1.graph.Subgraph_4.config.{
  Context => Subgraph_4_Context
}
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
    val df_SQLStatement_1 =
      SQLStatement_1(context,
                     df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "zTa3HjcrPr0ScL9QOW8BT$$M0QbyIk9v3R_9OpyenlDu",
                "1PGrDaLBabY__VcXs-VxW$$yrlYSve4Cnw5uVZQjLJpA"
      )
    df_SQLStatement_1.cache().count()
    df_SQLStatement_1.unpersist()
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
      SubgraphMain_Context(context.spark, context.config.SubgraphMain),
      df_src_parquet_all_type_and_partition_withspacehyphens1
    )
    val df_Reformat_1 = Reformat_1(context, df_SubgraphMain).interim(
      "graph",
      "9SxukrkbLjB9767nnjjyc$$HKrQPtYnAcfBjk1YLp12B",
      "q2OHFdKSlaApfiB-p2kod$$gjcVI8oVJu6r8jvarb7gm"
    )
    val df_src_jdbc_userandpass_test_table =
      src_jdbc_userandpass_test_table(context).interim(
        "graph",
        "7bWiJu3knHMbFj8nE2O8f$$Vfh9t2HhMUPhiC1sgoE9z",
        "oyv07gmYXA5N-nHgb867C$$_pfKTOFx4Zhsxjo1bZ7y1"
      )
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_src_parquet_all_type_and_partition_withspacehyphens1
    )
    val df_Subgraph_3 = Subgraph_3.apply(
      Subgraph_3_Context(context.spark, context.config.Subgraph_3),
      df_Subgraph_1
    )
    val df_Reformat_7 = Reformat_7(context, df_Subgraph_3).interim(
      "graph",
      "wuThqUc2qpf_FmJCMTj2e$$vhP6lu8CS3r_W42eJUyZ1",
      "H6Z_aAnftBpk4a-USAecW$$DfJIjJOqY8XnYd8OGH-ZP"
    )
    val df_Reformat_12 =
      Reformat_12(context, df_src_jdbc_userandpass_test_table).interim(
        "graph",
        "tmQl5lQn0cQySXaNaAVHW$$WYpBn368BPDBJgNS0dWDy",
        "eCME-E_UjLplbA6FShv8l$$bzW51Wn-Hll118g9zZHkk"
      )
    df_Reformat_12.cache().count()
    df_Reformat_12.unpersist()
    val df_Subgraph_4 = Subgraph_4.apply(
      Subgraph_4_Context(context.spark, context.config.Subgraph_4),
      df_Reformat_7
    )
    val df_Reformat_10 = Reformat_10(context, df_Subgraph_4).interim(
      "graph",
      "cxC3W007DVzCvlwaUSQWg$$ckcxE7qTOTY5b8BHciWQ2",
      "VJzZuiwZgT0rxBlvzkHAY$$80GGHEDnLkXPhO1zGDxko"
    )
    df_Reformat_10.cache().count()
    df_Reformat_10.unpersist()
    val df_Reformat_11 =
      Reformat_11(context,
                  df_src_parquet_all_type_and_partition_withspacehyphens1
      ).interim("graph",
                "wBnTj5gIWVWRXByB_Vj42$$_zH02tlhIB2gsjSBDPH__",
                "8szzm0jUStMGE_YkdoO_L$$Z_J2k0B1M3TU5sdgZB-UM"
      )
    df_Reformat_11.cache().count()
    df_Reformat_11.unpersist()
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
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_test", context.spark) {
        dest_test(context, df_Reformat_4)
      }
    }
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
    spark.conf.set("spark_config1", "spark_config_value_1")
    spark.conf.set("spark_config2", "spark_config_value_2")
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_BASIC_20_MINS")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config_value1")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",      "hadoop_config_value2")
    MetricsCollector.start(spark, "pipelines/SCALA_BASIC_20_MINS")
    graph(context)
    MetricsCollector.end(spark)
  }

}
