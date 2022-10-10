package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object all_type_scala_sg_1 {

  def apply(
    spark: SparkSession,
    in0:   DataFrame,
    in1:   DataFrame,
    in2:   DataFrame
  ): Subgraph3 = {
    val df_src_csv_all_type_no_partition_1 =
      src_csv_all_type_no_partition_1(spark).interim(
        "all_type_scala_sg_1",
        "src_csv_all_type_no_partition_1",
        "0NwLDePbq8XoET6rLo-n3$$Uvm9UP7hQLP4SNVdSWazS"
      )
    Lookup_1_1(spark, df_src_csv_all_type_no_partition_1)
    val df_Reformat_2_1 = Reformat_2_1(spark, in0).interim(
      "all_type_scala_sg_1",
      "Reformat_2_1",
      "IOGnwjIuO1YjIb-sLJyha$$z-m88Ih-4MuofF6x72-3l"
    )
    val df_Filter_1_1 = Filter_1_1(spark, df_Reformat_2_1).interim(
      "all_type_scala_sg_1",
      "Filter_1_1",
      "UULbpwesGwistLf0QlHPk$$YtgAts6E0a4nfRfpAQFnT"
    )
    val df_OrderBy_1_1 = OrderBy_1_1(spark, df_Filter_1_1).interim(
      "all_type_scala_sg_1",
      "OrderBy_1_1",
      "Ddaz4jJQzSi44fZRicVSW$$dACb-pEeKjRuZzSmirR03"
    )
    val df_Limit_1_1 = Limit_1_1(spark, df_OrderBy_1_1).interim(
      "all_type_scala_sg_1",
      "Limit_1_1",
      "g6spxFdnq3qgPqw265z3O$$TnlBMLQK4x1vkubSaLMKk"
    )
    val df_WindowFunction_1_1 = WindowFunction_1_1(spark, df_Limit_1_1).interim(
      "all_type_scala_sg_1",
      "WindowFunction_1_1",
      "gtWt2VSgvUB0J7hcxUL0n$$QKwGYTH68d1VkrBSH0dU5"
    )
    val df_SetOperation_1_1 = SetOperation_1_1(spark,
                                               df_WindowFunction_1_1,
                                               df_WindowFunction_1_1
    ).interim("all_type_scala_sg_1",
              "SetOperation_1_1",
              "Vd1zEA7MfS5nw7LTlqhDD$$mC1nOH0_rnG7RekIClcXo"
    )
    val df_SchemaTransform_1_1 =
      SchemaTransform_1_1(spark, df_SetOperation_1_1).interim(
        "all_type_scala_sg_1",
        "SchemaTransform_1_1",
        "xLJkxvbQqP8DuRgGsUUiP$$ib2bSfpt-XiNNtn198ZHJ"
      )
    val df_Join_1_1 =
      Join_1_1(spark, df_SchemaTransform_1_1, df_SchemaTransform_1_1).interim(
        "all_type_scala_sg_1",
        "Join_1_1",
        "RD_fpo-eYIkVlGHMOhi40$$tcQ3wxcvkOvswZk6WrV-r"
      )
    val (df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1) = {
      val (df_RowDistributor_1_1_out0_temp, df_RowDistributor_1_1_out1_temp) =
        RowDistributor_1_1(spark, df_Join_1_1)
      (df_RowDistributor_1_1_out0_temp.interim(
         "all_type_scala_sg_1",
         "RowDistributor_1_1",
         "HxaQX0nF88aUd633ycAPm$$xDbwDdLriA_ABtA3qYMIb"
       ),
       df_RowDistributor_1_1_out1_temp.interim(
         "all_type_scala_sg_1",
         "RowDistributor_1_1",
         "PwWx57iOUAbAqGLDKsfGQ$$W6aYvjrxQ4MaE5AUifyIs"
       )
      )
    }
    val df_Aggregate_1_1 =
      Aggregate_1_1(spark, df_RowDistributor_1_1_out0).interim(
        "all_type_scala_sg_1",
        "Aggregate_1_1",
        "JSwGY5d6RoYVcwEbme7Hq$$2VP80ojDY3nc7WLQ9_7UE"
      )
    val df_Deduplicate_1_1 = Deduplicate_1_1(spark, in1).interim(
      "all_type_scala_sg_1",
      "Deduplicate_1_1",
      "djg3J1fGmwlsK4U4kIhZ1$$2sgXnfeFAu-dTIA0r_mN1"
    )
    val df_Script_1_1 = Script_1_1(spark, df_Deduplicate_1_1).interim(
      "all_type_scala_sg_1",
      "Script_1_1",
      "cix9b7hiz5GT6SHK1a2ey$$M_p9K0N_yjTjl6eEal2kg"
    )
    val df_Reformat_7 = Reformat_7(spark, in2).interim(
      "all_type_scala_sg_1",
      "Reformat_7",
      "5LiZljahU3bU_Rym0A-1p$$5EnktdKLqT42j1JIco1cw"
    )
    df_Reformat_7.cache().count()
    df_Reformat_7.unpersist()
    val df_Reformat_1_1 =
      Reformat_1_1(spark, df_src_csv_all_type_no_partition_1).interim(
        "all_type_scala_sg_1",
        "Reformat_1_1",
        "y-_P0LZWdJ6sodhEZaaQ6$$9axgrkS6-bnnp4-zOmcyr"
      )
    df_Reformat_1_1.cache().count()
    df_Reformat_1_1.unpersist()
    val df_FlattenSchema_1_1 =
      FlattenSchema_1_1(spark, df_Aggregate_1_1).interim(
        "all_type_scala_sg_1",
        "FlattenSchema_1_1",
        "1TNe7-gys_ySzJscz90bs$$U5UKRBjPcsxN0mxhMpDcs"
      )
    val df_OrderBy_2_1 = OrderBy_2_1(spark, df_RowDistributor_1_1_out1).interim(
      "all_type_scala_sg_1",
      "OrderBy_2_1",
      "Nljh_h-Q4z_oo_ltnGgSe$$8WZQIha0hUl_nuHdx2aCl"
    )
    val df_recursive_1 = recursive_1.apply(spark, df_Script_1_1)
    (df_FlattenSchema_1_1, df_OrderBy_2_1, df_recursive_1)
  }

}
