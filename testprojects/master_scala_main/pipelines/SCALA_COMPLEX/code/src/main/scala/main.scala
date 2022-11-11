import io.prophecy.libs._
import config.ConfigStore._
import config._
import udfs.UDFs._
import udfs._
import graph._
import graph.SubGraph_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(spark: SparkSession): Unit = {
    val df_Source_13 = Source_13(spark).interim(
      "graph",
      "4c8lUyCOBMQXJt_5f7dbP$$hbMC_bA0Uq2x2q8FW4tAX",
      "pIPX9BUJ7XgmTvGgmV8Hu$$y6Sz1JeyfULGcGgi8slvs"
    )
    Lookup_2(spark, df_Source_13)
    val df_Source_5 = Source_5(spark).interim("graph",
                                              "QQ_f4SrARPaP0ZtL6uV0r",
                                              "FtKm0kyfkwIxab-8yOffM"
    )
    Lookup_1(spark, df_Source_5)
    val df_Reformat_4 = Reformat_4(spark, df_Source_13).interim(
      "graph",
      "QA8k3yf3NHL4Dj0WAvlcE$$6k8gyMJ_bK4ZvWD-WkPiK",
      "5857bxTW7G9sQZ8dVJJEf$$p25_pviQ-HeuDuRAg5Sv_"
    )
    df_Reformat_4.cache().count()
    df_Reformat_4.unpersist()
    val df_Source_9 = Source_9(spark).interim("graph",
                                              "xoDRuSduB1niuIF8PP3ct",
                                              "AJqWz1Up0SmtM70ipV65i"
    )
    val df_FlattenSchema_1 = FlattenSchema_1(spark, df_Source_9).interim(
      "graph",
      "Szohjp1gWxHYvFe2IrcM0$$UyKRJZMAGITZk2c-3nrry",
      "y0ljFftVNLexQytwgRtZe$$gCrSp9s-cpbqRZaLKOGRd"
    )
    val df_WindowFunction_1 = WindowFunction_1(spark, df_Source_13).interim(
      "graph",
      "kHjXzB0HTJD1XTuwrj5kw$$ufL5LdEj0VdfG7-f1lHjQ",
      "Xh1IgHG5x-W2wGzFMUf9N$$f3xyINARrBYxOtt8dLXCx"
    )
    val df_Deduplicate_1 = Deduplicate_1(spark, df_WindowFunction_1).interim(
      "graph",
      "vpuiPUloPPI5wKsdnBW2X$$9hYAzmyJqyva6xBUJQas7",
      "OcRhKi5A8GbNRmjSJ_cwb$$AAP0iEMiVDg3mvHXC8Y01"
    )
    val df_Source_3 = Source_3(spark)
      .interim("graph", "XM4cdlXB7oVFseHwX2LRg", "gB7zngP2OXebTsbxfm4vF")
      .cache()
    val df_Source_4 = Source_4(spark).interim("graph",
                                              "9MW3M6D_dPk34335bQHHC",
                                              "iZ223SCz_w9vGNJJkCIw-"
    )
    val df_Source_6 = Source_6(spark).interim("graph",
                                              "A7YBjCffwys4LPAleAKpC",
                                              "bsEyEzvvLYNF6CFSyeX9o"
    )
    val df_Source_7 = Source_7(spark).interim("graph",
                                              "nCk_vxm7W80JgQ2ANSHA_",
                                              "DqVz4ONoErO0RNOWar0yr"
    )
    val df_Source_8 = Source_8(spark).interim("graph",
                                              "osOfTnoQY4cOYhV6LnBYq",
                                              "pkhXK0L27Mh9R-Iq9ZFdV"
    )
    val df_Script_1 = Script_1(spark,
                               df_Source_4,
                               df_Source_5,
                               df_Source_6,
                               df_Source_3,
                               df_Source_7,
                               df_Source_8
    ).interim("graph", "ntYO_C9LBj2oXcT9ScxKC", "8hkvbnxtCb0LGV132c6CU")
    val df_Limit_2 = Limit_2(spark, df_FlattenSchema_1).interim(
      "graph",
      "P3wur35dmUpCUS9RI97B6$$C-aK17F4TT5ozfKLRxLlG",
      "CYHjgxwrG-UxafTq3mty0$$bE3Abq_bfr4CAfbtMpdRX"
    )
    val df_SubGraph_1 = SubGraph_1.apply(spark, df_Limit_2)
    val df_Source_0 = Source_0(spark)
      .interim("graph", "XGtSPUju4qs9iykiWDTrw", "czCQinKs65hflKumSBnjF")
      .cache()
    val df_Reformat_1 = Reformat_1(spark, df_Source_0).interim(
      "graph",
      "GmpasPHVARLxYcwTqet5w",
      "sp1puunJ_YBtTBXDilwG7"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    val df_Repartition_2 = Repartition_2(spark, df_WindowFunction_1).interim(
      "graph",
      "yfeifaX7xpRlj28Ls-8Vf$$29PiihbU95u1gXWccO3GA",
      "xIA_sDLdYIaSR9jB4VXlg$$Rv5ZSnqWe9vse2SDVvmu_"
    )
    val df_Repartition_3 = Repartition_3(spark, df_Repartition_2).interim(
      "graph",
      "COz-6QYHVMxEsZK7xgFjO$$s6pqEnlwUcNFDVBX2NtnT",
      "VE-plh40cOeg9G005TysE$$b0LQzqSl6POGMQ4j4vs3c"
    )
    df_Repartition_3.cache().count()
    df_Repartition_3.unpersist()
    val df_Source_2 = Source_2(spark)
      .interim("graph", "PM7sxRmKo0cGk1IYdBNtT", "zMejsXna2UN-uClx0tfKO")
      .cache()
    val df_ComplexExpression = ComplexExpression(spark, df_Source_2).interim(
      "graph",
      "2tRCXGkA-6TfjEnFofIJq",
      "MwSfNu4URv3g0PC7kc9CR"
    )
    val df_Script_2 = Script_2(spark, df_Source_4).interim(
      "graph",
      "h5XiG-HuGCGv4oeFsPFsp",
      "pOEwcWysgxUYA9RF4iwl-"
    )
    val df_OrderBy_3 = OrderBy_3(spark, df_ComplexExpression).interim(
      "graph",
      "hx5wO_87IAH8xNU8kd6u0$$NZNcKwMNB77oH_rUMhHw2",
      "3QwZLdav6axGl_0dxx9N_$$CvmPmUKZnuBwQ1mj-VrK0"
    )
    df_OrderBy_3.cache().count()
    df_OrderBy_3.unpersist()
    val df_Source_15 = Source_15(spark).interim(
      "graph",
      "vmRKV0Nd6-lh2PBG9DCyM$$r1IXlDV-WCE_ANbMUvQcs",
      "RiRMBmFnYhsz_jKLJYl4P$$UkVrz2ppXuwUgrdRSOXCa"
    )
    val df_Reformat_2 = Reformat_2(spark, df_Source_15).interim(
      "graph",
      "6Znk7A4h43eh2eyod9GFr$$FUM5E0P9Xrn_WDFN9qFVz",
      "r0a543LnVEGLj4EiHP7P0$$F6WweoNFKHeQEZHINdKwA"
    )
    withSubgraphName("graph", spark) {
      withTargetId("Target_3", spark) {
        Target_3(spark, df_Reformat_2)
      }
    }
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(spark, df_Deduplicate_1)
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
    val df_OrderBy_4 = OrderBy_4(spark, df_RowDistributor_1_out0).interim(
      "graph",
      "hWAQiNOptWzlVEkn-huuJ$$q-JVLZQnREXaW_I25BSj5",
      "Z4XurMdUJO-ETinTrSyh6$$CxMPDJXpvul_WH8wvOY5y"
    )
    df_OrderBy_4.cache().count()
    df_OrderBy_4.unpersist()
    val df_Source_1 = Source_1(spark)
      .interim("graph", "nEj64p7qzVS7z0LXXTFkx", "2G70-QEVG04zcV_iAsqv1")
      .cache()
    val df_Filter_1 = Filter_1(spark, df_Source_1).interim(
      "graph",
      "gNCO_k3OESC15dRefnTjD",
      "7ecj16KrYrMc5jCMOyV_-"
    )
    val df_Limit_1 = Limit_1(spark, df_Source_1).interim(
      "graph",
      "BtjgWEFk-IrCWsqN3RqDF",
      "vz8yOBdktTG02eFYUsCr_"
    )
    val df_OrderBy_1 = OrderBy_1(spark, df_Source_1).interim(
      "graph",
      "hxZRArGTe6IeA715uZ9hX",
      "NGuAUZuZAsYsYcFmYLp2B"
    )
    val df_Script_3 = Script_3(spark, df_Filter_1, df_Limit_1, df_OrderBy_1)
      .interim("graph", "NRY7mBrYHFd7nMbAND7yB", "_OQB7vQzBoBVAscfGCmoN")
    df_Script_3.cache().count()
    df_Script_3.unpersist()
    val df_Limit_3 = Limit_3(spark, df_RowDistributor_1_out1).interim(
      "graph",
      "Fveo5Vzi24BOiUGy55PZU$$9-p9OnU3R-r06567Tejez",
      "Ft6cGNpKOE2Nirw_XS9_R$$Cd22cRsabCIorU61swO2N"
    )
    df_Limit_3.cache().count()
    df_Limit_3.unpersist()
    val df_ConfigAndUDF = ConfigAndUDF(spark, df_Script_2).interim(
      "graph",
      "ryf6nWZatrJJgaQGDWPjC",
      "bY6dBUB7OHy6i8vc1uwbD"
    )
    val df_Filter_2 = Filter_2(spark, df_ConfigAndUDF).interim(
      "graph",
      "F_cxTDso7G28ruB0xni7N",
      "V_c7nCHezVT96rpb-8TzQ"
    )
    val df_OrderBy_2 = OrderBy_2(spark, df_Filter_2).interim(
      "graph",
      "0BEbuoCU7vasdz7Wr3Ft1",
      "1DyiAt45y3SZCoDDCNVmw"
    )
    df_OrderBy_2.cache().count()
    df_OrderBy_2.unpersist()
    val df_Source_14 = Source_14(spark).interim(
      "graph",
      "FMl2GuBQkrf6_L2-1_mJj$$StZaODbsxnN9fIdLwaO0z",
      "SmN3D2QHD52ep_LvBEm_y$$7ZYjSWohGfpYZvd31RArp"
    )
    withSubgraphName("graph", spark) {
      withTargetId("Target_2", spark) {
        Target_2(spark, df_Source_14)
      }
    }
    val df_Repartition_1 = Repartition_1(spark, df_Source_1).interim(
      "graph",
      "JDdGnXnYzeiw8aXJ5gB5q",
      "sNPPOS1ix-ZWOvC-Q0cPD"
    )
    df_Repartition_1.cache().count()
    df_Repartition_1.unpersist()
    val df_Source_11 = Source_11(spark).interim("graph",
                                                "Fmg6g-ViOm77hFxIpAPch",
                                                "ee0X8XILMHhyMro4U1_V7"
    )
    val df_Source_12 = Source_12(spark).interim("graph",
                                                "fps50t-jagt_48Vhl8R_D",
                                                "EiDMo5_AyEwcSacMRBlXH"
    )
    val df_Reformat_6 = Reformat_6(spark, df_Source_12).interim(
      "graph",
      "HdCZn-PAGTHH-KYQzUAS5",
      "ZunRJ0bGImVEWwFO3WSGy"
    )
    df_Reformat_6.cache().count()
    df_Reformat_6.unpersist()
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_SubGraph_1).interim(
      "graph",
      "X5J7daeiMxXXaSTwpkDu9$$bZ2O5xQ9NUIlbSkMbv1MY",
      "02JU61gNQIeeb7iKJc5Z5$$_IQVWieQ7Tfs3dWmwjS8M"
    )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()
    val df_SetOperation_1 =
      SetOperation_1(spark, df_Source_11, df_Source_11).interim(
        "graph",
        "6MGeoO_3CAkDyZvPYAGOY$$W7ZInyQSxiNrP-XuCd9hK",
        "9nSC-MM8SEHYGTWCQwmW8$$82qHa1CcCeW1MGcYp5jb4"
      )
    val df_Join_1 = Join_1(spark, df_Source_1, df_Source_1).interim(
      "graph",
      "s6VHxJpslzbkbawETR2P-",
      "Vgk6IkOJ1X4GCgsQt0iEM"
    )
    df_Join_1.cache().count()
    df_Join_1.unpersist()
    Script_4(spark, df_SetOperation_1)
    val df_Reformat_8 = Reformat_8(spark, df_Script_1).interim(
      "graph",
      "VojrEOLLM7nesHVbUffgG$$ujNbJd_Qe--3wvxRqPV3F",
      "USRNNEY4yPLG763IDrN7e$$ALfX5lz8yMMHXG3htDpVy"
    )
    df_Reformat_8.cache().count()
    df_Reformat_8.unpersist()
    val df_Reformat_7 = Reformat_7(spark, df_Source_4).interim(
      "graph",
      "PtmsIQAqn36XZLBWoBd_D",
      "Gr4jS0OTmx6qz1pCr1LRc"
    )
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
    val df_Reformat_3 = Reformat_3(spark, df_Source_1).interim(
      "graph",
      "5jhe2NMutKCmtfsWW03Dh",
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
    spark.conf.set("spark_config1",                  "spark_config1_value")
    spark.conf.set("spark_config2",                  "spark_config2_value")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_COMPLEX")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config1", "hadoop_config1_value")
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2",      "hadoop_config2_value")
    MetricsCollector.start(spark, "pipelines/SCALA_COMPLEX")
    graph(spark)
    MetricsCollector.end(spark)
  }

}
