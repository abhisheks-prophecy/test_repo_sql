import io.prophecy.libs._
import config.ConfigStore._
import config._
import udfs.UDFs._
import udfs._
import graph._
import graph.SubGraph_1
import graph.everythingSG_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Main {

  def graph(spark: SparkSession): Unit = {
    val df_Source_5 =
      Source_5(spark).interim("graph", "Source_5", "FtKm0kyfkwIxab-8yOffM")
    Lookup_2(spark,                    df_Source_5)
    val df_Source_9 =
      Source_9(spark).interim("graph", "Source_9", "AJqWz1Up0SmtM70ipV65i")
    val df_FlattenSchema_1 = FlattenSchema_1(spark, df_Source_9).interim(
      "graph",
      "FlattenSchema_1",
      "y0ljFftVNLexQytwgRtZe$$gCrSp9s-cpbqRZaLKOGRd"
    )
    val df_all_type_parquet = all_type_parquet(spark).interim(
      "graph",
      "all_type_parquet",
      "pIPX9BUJ7XgmTvGgmV8Hu$$y6Sz1JeyfULGcGgi8slvs"
    )
    val df_WindowFunction_1 =
      WindowFunction_1(spark, df_all_type_parquet).interim(
        "graph",
        "WindowFunction_1",
        "Xh1IgHG5x-W2wGzFMUf9N$$f3xyINARrBYxOtt8dLXCx"
      )
    val df_Deduplicate_1 = Deduplicate_1(spark, df_WindowFunction_1).interim(
      "graph",
      "Deduplicate_1",
      "OcRhKi5A8GbNRmjSJ_cwb$$AAP0iEMiVDg3mvHXC8Y01"
    )
    val df_Source_3 = Source_3(spark)
      .interim("graph", "Source_3", "gB7zngP2OXebTsbxfm4vF")
      .cache()
    val df_Source_4 =
      Source_4(spark).interim("graph", "Source_4", "iZ223SCz_w9vGNJJkCIw-")
    val df_Source_6 =
      Source_6(spark).interim("graph", "Source_6", "bsEyEzvvLYNF6CFSyeX9o")
    val df_Source_7 =
      Source_7(spark).interim("graph", "Source_7", "DqVz4ONoErO0RNOWar0yr")
    val df_Source_8 =
      Source_8(spark).interim("graph", "Source_8", "pkhXK0L27Mh9R-Iq9ZFdV")
    val df_Script_1 = Script_1(spark,
                               df_Source_4,
                               df_Source_5,
                               df_Source_6,
                               df_Source_3,
                               df_Source_7,
                               df_Source_8
    ).interim("graph", "Script_1", "8hkvbnxtCb0LGV132c6CU")
    val df_Limit_2 = Limit_2(spark, df_FlattenSchema_1).interim(
      "graph",
      "Limit_2",
      "CYHjgxwrG-UxafTq3mty0$$bE3Abq_bfr4CAfbtMpdRX"
    )
    val df_SubGraph_1 = SubGraph_1.apply(spark, df_Limit_2)
    val df_Repartition_2 = Repartition_2(spark, df_WindowFunction_1).interim(
      "graph",
      "Repartition_2",
      "xIA_sDLdYIaSR9jB4VXlg$$Rv5ZSnqWe9vse2SDVvmu_"
    )
    val df_Repartition_3 = Repartition_3(spark, df_Repartition_2).interim(
      "graph",
      "Repartition_3",
      "VE-plh40cOeg9G005TysE$$b0LQzqSl6POGMQ4j4vs3c"
    )
    df_Repartition_3.cache().count()
    df_Repartition_3.unpersist()
    val df_Source_2 = Source_2(spark)
      .interim("graph", "Source_2", "zMejsXna2UN-uClx0tfKO")
      .cache()
    val df_ComplexExpression = ComplexExpression(spark, df_Source_2).interim(
      "graph",
      "ComplexExpression",
      "MwSfNu4URv3g0PC7kc9CR"
    )
    val df_Script_2 = Script_2(spark, df_Source_4).interim(
      "graph",
      "Script_2",
      "pOEwcWysgxUYA9RF4iwl-"
    )
    val df_OrderBy_3 = OrderBy_3(spark, df_ComplexExpression).interim(
      "graph",
      "OrderBy_3",
      "3QwZLdav6axGl_0dxx9N_$$CvmPmUKZnuBwQ1mj-VrK0"
    )
    df_OrderBy_3.cache().count()
    df_OrderBy_3.unpersist()
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(spark, df_Deduplicate_1)
      (df_RowDistributor_1_out0_temp.interim("graph",
                                             "RowDistributor_1",
                                             "out0"
       ),
       df_RowDistributor_1_out1_temp.interim("graph",
                                             "RowDistributor_1",
                                             "out1"
       )
      )
    }
    val df_OrderBy_4 = OrderBy_4(spark, df_RowDistributor_1_out0).interim(
      "graph",
      "OrderBy_4",
      "Z4XurMdUJO-ETinTrSyh6$$CxMPDJXpvul_WH8wvOY5y"
    )
    df_OrderBy_4.cache().count()
    df_OrderBy_4.unpersist()
    val df_Source_1 = Source_1(spark)
      .interim("graph", "Source_1", "2G70-QEVG04zcV_iAsqv1")
      .cache()
    val df_Filter_1 = Filter_1(spark, df_Source_1).interim(
      "graph",
      "Filter_1",
      "7ecj16KrYrMc5jCMOyV_-"
    )
    val df_Limit_1 = Limit_1(spark, df_Source_1).interim("graph",
                                                         "Limit_1",
                                                         "vz8yOBdktTG02eFYUsCr_"
    )
    val df_OrderBy_1 = OrderBy_1(spark, df_Source_1).interim(
      "graph",
      "OrderBy_1",
      "NGuAUZuZAsYsYcFmYLp2B"
    )
    val df_Script_3 = Script_3(spark, df_Filter_1, df_Limit_1, df_OrderBy_1)
      .interim("graph", "Script_3", "_OQB7vQzBoBVAscfGCmoN")
    df_Script_3.cache().count()
    df_Script_3.unpersist()
    val df_Limit_3 = Limit_3(spark, df_RowDistributor_1_out1).interim(
      "graph",
      "Limit_3",
      "Ft6cGNpKOE2Nirw_XS9_R$$Cd22cRsabCIorU61swO2N"
    )
    df_Limit_3.cache().count()
    df_Limit_3.unpersist()
    val df_ConfigAndUDF = ConfigAndUDF(spark, df_Script_2).interim(
      "graph",
      "ConfigAndUDF",
      "bY6dBUB7OHy6i8vc1uwbD"
    )
    val df_Filter_2 = Filter_2(spark, df_ConfigAndUDF).interim(
      "graph",
      "Filter_2",
      "V_c7nCHezVT96rpb-8TzQ"
    )
    val df_OrderBy_2 = OrderBy_2(spark, df_Filter_2).interim(
      "graph",
      "OrderBy_2",
      "1DyiAt45y3SZCoDDCNVmw"
    )
    df_OrderBy_2.cache().count()
    df_OrderBy_2.unpersist()
    val df_custom_xlsx_scala = custom_xlsx_scala(spark).interim(
      "graph",
      "custom_xlsx_scala",
      "C3NJaHDCgTRF-J9WkzEJd$$sW-36fUL4v1xl97cqV2Y-"
    )
    val df_Reformat_4 = Reformat_4(spark, df_custom_xlsx_scala).interim(
      "graph",
      "Reformat_4",
      "1W14003bJswIwHtP2sXfj$$Ce_vmK8xATF1kmk7ousPi"
    )
    df_Reformat_4.cache().count()
    df_Reformat_4.unpersist()
    val df_Repartition_1 = Repartition_1(spark, df_Source_1).interim(
      "graph",
      "Repartition_1",
      "sNPPOS1ix-ZWOvC-Q0cPD"
    )
    df_Repartition_1.cache().count()
    df_Repartition_1.unpersist()
    val df_Source_11 =
      Source_11(spark).interim("graph", "Source_11", "ee0X8XILMHhyMro4U1_V7")
    val df_Source_15 = Source_15(spark).interim(
      "graph",
      "Source_15",
      "RiRMBmFnYhsz_jKLJYl4P$$UkVrz2ppXuwUgrdRSOXCa"
    )
    val df_Reformat_2 = Reformat_2(spark, df_Source_15).interim(
      "graph",
      "Reformat_2",
      "r0a543LnVEGLj4EiHP7P0$$F6WweoNFKHeQEZHINdKwA"
    )
    df_Reformat_2.cache().count()
    df_Reformat_2.unpersist()
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_SubGraph_1).interim(
      "graph",
      "SchemaTransform_1",
      "02JU61gNQIeeb7iKJc5Z5$$_IQVWieQ7Tfs3dWmwjS8M"
    )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()
    val df_SetOperation_1 =
      SetOperation_1(spark, df_Source_11, df_Source_11).interim(
        "graph",
        "SetOperation_1",
        "9nSC-MM8SEHYGTWCQwmW8$$82qHa1CcCeW1MGcYp5jb4"
      )
    val df_Join_1 = Join_1(spark, df_Source_1, df_Source_1).interim(
      "graph",
      "Join_1",
      "Vgk6IkOJ1X4GCgsQt0iEM"
    )
    df_Join_1.cache().count()
    df_Join_1.unpersist()
    Script_4(spark, df_SetOperation_1)
    val (df_everythingSG_1_out0,
         df_everythingSG_1_out1,
         df_everythingSG_1_out2,
         df_everythingSG_1_out3
    ) = {
      val (df_everythingSG_1_out0_temp,
           df_everythingSG_1_out1_temp,
           df_everythingSG_1_out2_temp,
           df_everythingSG_1_out3_temp
      ) = everythingSG_1.apply(spark,
                               df_all_type_parquet,
                               df_all_type_parquet,
                               df_all_type_parquet
      )
      (df_everythingSG_1_out0_temp,
       df_everythingSG_1_out1_temp,
       df_everythingSG_1_out2_temp,
       df_everythingSG_1_out3_temp
      )
    }
    val df_Script_5 = Script_5(spark,
                               df_everythingSG_1_out0,
                               df_everythingSG_1_out1,
                               df_everythingSG_1_out2,
                               df_everythingSG_1_out3
    ).interim("graph",
              "Script_5",
              "1JtuFC7vRxCs9lBruBSNR$$L66u-4UA2FQ-8W5AS58-d"
    )
    val df_Reformat_8 = Reformat_8(spark, df_Script_1).interim(
      "graph",
      "Reformat_8",
      "USRNNEY4yPLG763IDrN7e$$ALfX5lz8yMMHXG3htDpVy"
    )
    df_Reformat_8.cache().count()
    df_Reformat_8.unpersist()
    val df_Reformat_7 = Reformat_7(spark, df_Script_5).interim(
      "graph",
      "Reformat_7",
      "6_iM00WB8hZdwnO3caZyU$$ArJ833Xcbh66jXZjJk9xq"
    )
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
    val df_Reformat_3 = Reformat_3(spark, df_Source_1).interim(
      "graph",
      "Reformat_3",
      "SzVxzITqrWe5YCf4ZUe7t"
    )
    df_Reformat_3.cache().count()
    df_Reformat_3.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("spark_config1", "spark_config1_value")
    spark.conf.set("spark_config2", "spark_config2_value")
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "6148/pipelines/DONOT_OPEN_SCALA")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config1_value")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",      "hadoop_config2_value")
    MetricsCollector.start(spark, "6148/pipelines/DONOT_OPEN_SCALA")
    graph(spark)
    MetricsCollector.end(spark)
  }

}
