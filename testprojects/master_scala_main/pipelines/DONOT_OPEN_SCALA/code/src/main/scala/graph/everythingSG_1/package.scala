package graph

import io.prophecy.libs._
import config.ConfigStore._
import config._
import graph.everythingSG_1.recursive
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object everythingSG_1 {

  def apply(
    spark: SparkSession,
    in0:   DataFrame,
    in1:   DataFrame,
    in2:   DataFrame
  ): Subgraph4 = {
    val df_src_csv_all_type_no_partition =
      src_csv_all_type_no_partition(spark).interim(
        "everythingSG_1",
        "src_csv_all_type_no_partition",
        "AkcClQlLEjW2_hZLVHd_K$$SA-cROB8PXlFV8Wu7PKnF"
      )
    Lookup_1(spark, df_src_csv_all_type_no_partition)
    val df_Deduplicate_1 = Deduplicate_1(spark, in1).interim(
      "everythingSG_1",
      "Deduplicate_1",
      "Jjk3m71fE08G6q2zh26ZY$$c7vBF9LBXOj8Sp0l_6cyC"
    )
    val df_Script_1 = Script_1(spark, df_Deduplicate_1).interim(
      "everythingSG_1",
      "Script_1",
      "YCc7Bg9MF6bugh1dQnsfq$$emAGCBCiAuXT_3xnR5lAu"
    )
    val df_recursive = recursive.apply(spark, df_Script_1)
    val df_Reformat_2 = Reformat_2(spark, in0).interim(
      "everythingSG_1",
      "Reformat_2",
      "LLew197ZSyrWJ2fjA9C6x$$usoHZqM4EAM0SPdubW248"
    )
    val df_Filter_1 = Filter_1(spark, df_Reformat_2).interim(
      "everythingSG_1",
      "Filter_1",
      "6vGXt22DPL9pliXyv085d$$WBiKdlYVQSR6QM_qVmgtR"
    )
    val df_OrderBy_1 = OrderBy_1(spark, df_Filter_1).interim(
      "everythingSG_1",
      "OrderBy_1",
      "FRO7Stm4Ff7dh8Xa6fC0w$$G_QbxFH3Mdn5Dzz9jJzLS"
    )
    val df_Limit_1 = Limit_1(spark, df_OrderBy_1).interim(
      "everythingSG_1",
      "Limit_1",
      "NRcYDFEfMdGRJOkB42L4s$$D7cUafvXyKDnwX_XADMv6"
    )
    val df_WindowFunction_1 = WindowFunction_1(spark, df_Limit_1).interim(
      "everythingSG_1",
      "WindowFunction_1",
      "nmvAoqvzzBrF5cIxBRAI1$$v5iVyjx5k9-UOp6iJpsjC"
    )
    val df_SetOperation_1 =
      SetOperation_1(spark, df_WindowFunction_1, df_WindowFunction_1).interim(
        "everythingSG_1",
        "SetOperation_1",
        "v7x2C2MRazGWjKkQERttd$$2SSatSnowt4bednaZwF_0"
      )
    val df_SchemaTransform_1 =
      SchemaTransform_1(spark, df_SetOperation_1).interim(
        "everythingSG_1",
        "SchemaTransform_1",
        "MjDnwoouFjkC0ZsN-GcRZ$$pAXcYJ820F1dStXDMIWqH"
      )
    val df_Join_1 =
      Join_1(spark, df_SchemaTransform_1, df_SchemaTransform_1).interim(
        "everythingSG_1",
        "Join_1",
        "aEL_fDC7jPCTy4wXza8-v$$1fx3FR5WwtlLGCEFxMFqy"
      )
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(spark, df_Join_1)
      (df_RowDistributor_1_out0_temp.interim("everythingSG_1",
                                             "RowDistributor_1",
                                             "out0$$p2Sd3MEnceSSdz9012q3O"
       ),
       df_RowDistributor_1_out1_temp.interim("everythingSG_1",
                                             "RowDistributor_1",
                                             "out1$$brppcRJ4OEoAWPesxLwAV"
       )
      )
    }
    val df_Reformat_1 =
      Reformat_1(spark, df_src_csv_all_type_no_partition).interim(
        "everythingSG_1",
        "Reformat_1",
        "W3MRq-d9zKu8u1rJGpB55$$WjgM43hYdsgmV2JI7rdYQ"
      )
    withSubgraphName("everythingSG_1", spark) {
      withTargetId("dest_12345", spark) {
        dest_12345(spark, df_Reformat_1)
      }
    }
    val df_Aggregate_1 = Aggregate_1(spark, df_RowDistributor_1_out0).interim(
      "everythingSG_1",
      "Aggregate_1",
      "JlARlNTpqpXju3Yb5n7Jb$$dexkwVIzrWlt4LFbKox4F"
    )
    val df_OrderBy_2 = OrderBy_2(spark, df_RowDistributor_1_out1).interim(
      "everythingSG_1",
      "OrderBy_2",
      "YM9zrAKAXA7H536kn3q38$$J9voViO87JwI2b7SDfQ9b"
    )
    val df_FlattenSchema_1 = FlattenSchema_1(spark, df_Aggregate_1).interim(
      "everythingSG_1",
      "FlattenSchema_1",
      "01HxfgL5Kj9sroiuQyzHJ$$BiCTgt30b8Zpn9oQaQZNq"
    )
    (df_Reformat_1, df_FlattenSchema_1, df_OrderBy_2, df_recursive)
  }

}
