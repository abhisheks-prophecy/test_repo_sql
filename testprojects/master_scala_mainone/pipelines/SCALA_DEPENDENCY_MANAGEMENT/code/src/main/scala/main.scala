import io.prophecy.libs._
import config.ConfigStore._
import config.Context
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
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_dataset_cust_in_1 = dataset_cust_in_1(context).interim(
      "graph",
      "QQ_f4SrARPaP0ZtL6uV0r",
      "FtKm0kyfkwIxab-8yOffM"
    )
    Lookup_2(context, df_dataset_cust_in_1)
    val df_src_orc_all_type_no_partition = src_orc_all_type_no_partition(
      context
    ).interim("graph", "xoDRuSduB1niuIF8PP3ct", "AJqWz1Up0SmtM70ipV65i")
    val df_FlattenSchema_1 =
      FlattenSchema_1(context, df_src_orc_all_type_no_partition).interim(
        "graph",
        "Szohjp1gWxHYvFe2IrcM0$$UyKRJZMAGITZk2c-3nrry",
        "y0ljFftVNLexQytwgRtZe$$gCrSp9s-cpbqRZaLKOGRd"
      )
    val df_all_type_parquet = all_type_parquet(context).interim(
      "graph",
      "4c8lUyCOBMQXJt_5f7dbP$$hbMC_bA0Uq2x2q8FW4tAX",
      "pIPX9BUJ7XgmTvGgmV8Hu$$y6Sz1JeyfULGcGgi8slvs"
    )
    val df_WindowFunction_1 =
      WindowFunction_1(context, df_all_type_parquet).interim(
        "graph",
        "kHjXzB0HTJD1XTuwrj5kw$$ufL5LdEj0VdfG7-f1lHjQ",
        "Xh1IgHG5x-W2wGzFMUf9N$$f3xyINARrBYxOtt8dLXCx"
      )
    val df_Deduplicate_1 = Deduplicate_1(context, df_WindowFunction_1).interim(
      "graph",
      "vpuiPUloPPI5wKsdnBW2X$$9hYAzmyJqyva6xBUJQas7",
      "OcRhKi5A8GbNRmjSJ_cwb$$AAP0iEMiVDg3mvHXC8Y01"
    )
    val df_src_avro_CustsDatasetInput = src_avro_CustsDatasetInput(context)
      .interim("graph", "XM4cdlXB7oVFseHwX2LRg", "gB7zngP2OXebTsbxfm4vF")
      .cache()
    val df_dataset_cust_in = dataset_cust_in(context).interim(
      "graph",
      "9MW3M6D_dPk34335bQHHC",
      "iZ223SCz_w9vGNJJkCIw-"
    )
    val df_dataset_cust_in_4 = dataset_cust_in_4(context).interim(
      "graph",
      "A7YBjCffwys4LPAleAKpC",
      "bsEyEzvvLYNF6CFSyeX9o"
    )
    val df_dataset_cust_in_3 = dataset_cust_in_3(context).interim(
      "graph",
      "nCk_vxm7W80JgQ2ANSHA_",
      "DqVz4ONoErO0RNOWar0yr"
    )
    val df_dataset_cust_in_2 = dataset_cust_in_2(context).interim(
      "graph",
      "osOfTnoQY4cOYhV6LnBYq",
      "pkhXK0L27Mh9R-Iq9ZFdV"
    )
    val df_Script_1 = Script_1(context,
                               df_dataset_cust_in,
                               df_dataset_cust_in_1,
                               df_dataset_cust_in_4,
                               df_src_avro_CustsDatasetInput,
                               df_dataset_cust_in_3,
                               df_dataset_cust_in_2
    ).interim("graph", "ntYO_C9LBj2oXcT9ScxKC", "8hkvbnxtCb0LGV132c6CU")
    val df_Limit_2 = Limit_2(context, df_FlattenSchema_1).interim(
      "graph",
      "P3wur35dmUpCUS9RI97B6$$C-aK17F4TT5ozfKLRxLlG",
      "CYHjgxwrG-UxafTq3mty0$$bE3Abq_bfr4CAfbtMpdRX"
    )
    val df_SubGraph_1 = SubGraph_1.apply(context, df_Limit_2)
    val df_Repartition_2 = Repartition_2(context, df_WindowFunction_1).interim(
      "graph",
      "yfeifaX7xpRlj28Ls-8Vf$$29PiihbU95u1gXWccO3GA",
      "xIA_sDLdYIaSR9jB4VXlg$$Rv5ZSnqWe9vse2SDVvmu_"
    )
    val df_Repartition_3 = Repartition_3(context, df_Repartition_2).interim(
      "graph",
      "COz-6QYHVMxEsZK7xgFjO$$s6pqEnlwUcNFDVBX2NtnT",
      "VE-plh40cOeg9G005TysE$$b0LQzqSl6POGMQ4j4vs3c"
    )
    df_Repartition_3.cache().count()
    df_Repartition_3.unpersist()
    val df_src_parquet_all_type_no_partition =
      src_parquet_all_type_no_partition(context)
        .interim("graph", "PM7sxRmKo0cGk1IYdBNtT", "zMejsXna2UN-uClx0tfKO")
        .cache()
    val df_ComplexExpression = ComplexExpression(
      context,
      df_src_parquet_all_type_no_partition
    ).interim("graph", "2tRCXGkA-6TfjEnFofIJq", "MwSfNu4URv3g0PC7kc9CR")
    val df_Script_2 = Script_2(context, df_dataset_cust_in).interim(
      "graph",
      "h5XiG-HuGCGv4oeFsPFsp",
      "pOEwcWysgxUYA9RF4iwl-"
    )
    val df_OrderBy_3 = OrderBy_3(context, df_ComplexExpression).interim(
      "graph",
      "hx5wO_87IAH8xNU8kd6u0$$NZNcKwMNB77oH_rUMhHw2",
      "3QwZLdav6axGl_0dxx9N_$$CvmPmUKZnuBwQ1mj-VrK0"
    )
    df_OrderBy_3.cache().count()
    df_OrderBy_3.unpersist()
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_Deduplicate_1)
      (df_RowDistributor_1_out0_temp.interim(
         "graph",
         "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
         "out0"
       ),
       df_RowDistributor_1_out1_temp.interim(
         "graph",
         "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
         "out1"
       )
      )
    }
    val df_OrderBy_4 = OrderBy_4(context, df_RowDistributor_1_out0).interim(
      "graph",
      "hWAQiNOptWzlVEkn-huuJ$$q-JVLZQnREXaW_I25BSj5",
      "Z4XurMdUJO-ETinTrSyh6$$CxMPDJXpvul_WH8wvOY5y"
    )
    df_OrderBy_4.cache().count()
    df_OrderBy_4.unpersist()
    val df_src_csv_special_char_column_name = src_csv_special_char_column_name(
      context
    ).interim("graph", "nEj64p7qzVS7z0LXXTFkx", "2G70-QEVG04zcV_iAsqv1").cache()
    val df_Filter_1 = Filter_1(context, df_src_csv_special_char_column_name)
      .interim("graph", "gNCO_k3OESC15dRefnTjD", "7ecj16KrYrMc5jCMOyV_-")
    val df_Limit_1 = Limit_1(context, df_src_csv_special_char_column_name)
      .interim("graph", "BtjgWEFk-IrCWsqN3RqDF", "vz8yOBdktTG02eFYUsCr_")
    val df_OrderBy_1 = OrderBy_1(context, df_src_csv_special_char_column_name)
      .interim("graph", "hxZRArGTe6IeA715uZ9hX", "NGuAUZuZAsYsYcFmYLp2B")
    val df_Script_3 = Script_3(context, df_Filter_1, df_Limit_1, df_OrderBy_1)
      .interim("graph", "NRY7mBrYHFd7nMbAND7yB", "_OQB7vQzBoBVAscfGCmoN")
    df_Script_3.cache().count()
    df_Script_3.unpersist()
    val df_Limit_3 = Limit_3(context, df_RowDistributor_1_out1).interim(
      "graph",
      "Fveo5Vzi24BOiUGy55PZU$$9-p9OnU3R-r06567Tejez",
      "Ft6cGNpKOE2Nirw_XS9_R$$Cd22cRsabCIorU61swO2N"
    )
    df_Limit_3.cache().count()
    df_Limit_3.unpersist()
    val df_ConfigAndUDF = ConfigAndUDF(context, df_Script_2).interim(
      "graph",
      "ryf6nWZatrJJgaQGDWPjC",
      "bY6dBUB7OHy6i8vc1uwbD"
    )
    val df_Filter_2 = Filter_2(context, df_ConfigAndUDF).interim(
      "graph",
      "F_cxTDso7G28ruB0xni7N",
      "V_c7nCHezVT96rpb-8TzQ"
    )
    val df_OrderBy_2 = OrderBy_2(context, df_Filter_2).interim(
      "graph",
      "0BEbuoCU7vasdz7Wr3Ft1",
      "1DyiAt45y3SZCoDDCNVmw"
    )
    val df_src_jdbc_dbsecrets_test_table =
      src_jdbc_dbsecrets_test_table(context).interim(
        "graph",
        "FMl2GuBQkrf6_L2-1_mJj$$StZaODbsxnN9fIdLwaO0z",
        "SmN3D2QHD52ep_LvBEm_y$$7ZYjSWohGfpYZvd31RArp"
      )
    withSubgraphName("graph", context.spark) {
      withTargetId("dest_jdbc_dbsecrets_test_table", context.spark) {
        dest_jdbc_dbsecrets_test_table(context,
                                       df_src_jdbc_dbsecrets_test_table
        )
      }
    }
    val df_Repartition_1 = Repartition_1(context,
                                         df_src_csv_special_char_column_name
    ).interim("graph", "JDdGnXnYzeiw8aXJ5gB5q", "sNPPOS1ix-ZWOvC-Q0cPD")
    df_Repartition_1.cache().count()
    df_Repartition_1.unpersist()
    val df_src_delta_all_type_no_partition = src_delta_all_type_no_partition(
      context
    ).interim("graph", "Fmg6g-ViOm77hFxIpAPch", "ee0X8XILMHhyMro4U1_V7")
    val df_src_catalog_table_test_catalog_source =
      src_catalog_table_test_catalog_source(context).interim(
        "graph",
        "vmRKV0Nd6-lh2PBG9DCyM$$r1IXlDV-WCE_ANbMUvQcs",
        "RiRMBmFnYhsz_jKLJYl4P$$UkVrz2ppXuwUgrdRSOXCa"
      )
    val df_Reformat_2 =
      Reformat_2(context, df_src_catalog_table_test_catalog_source).interim(
        "graph",
        "6Znk7A4h43eh2eyod9GFr$$FUM5E0P9Xrn_WDFN9qFVz",
        "r0a543LnVEGLj4EiHP7P0$$F6WweoNFKHeQEZHINdKwA"
      )
    df_Reformat_2.cache().count()
    df_Reformat_2.unpersist()
    val df_SchemaTransform_1 =
      SchemaTransform_1(context, df_SubGraph_1).interim(
        "graph",
        "X5J7daeiMxXXaSTwpkDu9$$bZ2O5xQ9NUIlbSkMbv1MY",
        "02JU61gNQIeeb7iKJc5Z5$$_IQVWieQ7Tfs3dWmwjS8M"
      )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()
    val df_SetOperation_1 = SetOperation_1(context,
                                           df_src_delta_all_type_no_partition,
                                           df_src_delta_all_type_no_partition
    ).interim("graph",
              "6MGeoO_3CAkDyZvPYAGOY$$W7ZInyQSxiNrP-XuCd9hK",
              "9nSC-MM8SEHYGTWCQwmW8$$82qHa1CcCeW1MGcYp5jb4"
    )
    val df_Join_1 = Join_1(context,
                           df_src_csv_special_char_column_name,
                           df_src_csv_special_char_column_name
    ).interim("graph", "s6VHxJpslzbkbawETR2P-", "Vgk6IkOJ1X4GCgsQt0iEM")
    df_Join_1.cache().count()
    df_Join_1.unpersist()
    Script_4(context, df_SetOperation_1)
    val (df_SQLStatement_1_out, df_SQLStatement_1_out1) = {
      val (df_SQLStatement_1_out_temp, df_SQLStatement_1_out1_temp) =
        SQLStatement_1(context, df_OrderBy_2)
      (df_SQLStatement_1_out_temp.interim(
         "graph",
         "fn4JrOL1670M5lMw8tDwS$$hx9jyx_1q4axD3qTeDPuN",
         "HIAzmUBvAuY06e7zVKea-$$1sUUSztQfxfgEeLZnXJCU"
       ),
       df_SQLStatement_1_out1_temp.interim(
         "graph",
         "fn4JrOL1670M5lMw8tDwS$$hx9jyx_1q4axD3qTeDPuN",
         "p2K-Fbo4t_uOj5y-DZlr5$$Cn8AVElyltmzC1v1Wk314"
       )
      )
    }
    df_SQLStatement_1_out1.cache().count()
    df_SQLStatement_1_out1.unpersist()
    val df_Filter_3 = Filter_3(context, df_SQLStatement_1_out).interim(
      "graph",
      "pgsLYrwBU-xJefcYNjdop$$FVmUmvevvKjkQbybNCrCX",
      "xcV8q1WDg1H5LlxglSk_3$$NpPF4lWJWyFgVGi9PMTzE"
    )
    df_Filter_3.cache().count()
    df_Filter_3.unpersist()
    val (df_everythingSG_1_out0,
         df_everythingSG_1_out1,
         df_everythingSG_1_out2,
         df_everythingSG_1_out3
    ) = {
      val (df_everythingSG_1_out0_temp,
           df_everythingSG_1_out1_temp,
           df_everythingSG_1_out2_temp,
           df_everythingSG_1_out3_temp
      ) = everythingSG_1.apply(context,
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
    val df_Script_5 = Script_5(context,
                               df_everythingSG_1_out0,
                               df_everythingSG_1_out1,
                               df_everythingSG_1_out2,
                               df_everythingSG_1_out3
    ).interim("graph",
              "nQiz2d6SRIARVLzvC1odD$$pbN4wtXHyLQPsfLa3oD2E",
              "1JtuFC7vRxCs9lBruBSNR$$L66u-4UA2FQ-8W5AS58-d"
    )
    val df_Reformat_8 = Reformat_8(context, df_Script_1).interim(
      "graph",
      "VojrEOLLM7nesHVbUffgG$$ujNbJd_Qe--3wvxRqPV3F",
      "USRNNEY4yPLG763IDrN7e$$ALfX5lz8yMMHXG3htDpVy"
    )
    df_Reformat_8.cache().count()
    df_Reformat_8.unpersist()
    val df_Reformat_7 = Reformat_7(context, df_Script_5).interim(
      "graph",
      "asWfDvNmm7Zv4jeyl_Iar$$0Tohoz1tm7PCjn6AviQcn",
      "6_iM00WB8hZdwnO3caZyU$$ArJ833Xcbh66jXZjJk9xq"
    )
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
    val df_Reformat_3 = Reformat_3(context, df_src_csv_special_char_column_name)
      .interim("graph", "5jhe2NMutKCmtfsWW03Dh", "SzVxzITqrWe5YCf4ZUe7t")
    df_Reformat_3.cache().count()
    df_Reformat_3.unpersist()
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
    spark.conf.set("spark_config1", "spark_config1_value")
    spark.conf.set("spark_config2", "spark_config2_value")
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/SCALA_DEPENDENCY_MANAGEMENT"
    )
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config1_value")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",      "hadoop_config2_value")
    MetricsCollector.start(spark, "pipelines/SCALA_DEPENDENCY_MANAGEMENT")
    graph(context)
    MetricsCollector.end(spark)
  }

}
