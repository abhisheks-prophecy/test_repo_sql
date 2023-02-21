package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import config._
import graph.everythingSG_1.recursive
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object everythingSG_1 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): Subgraph4 = {
    val df_src_csv_all_type_no_partition =
      src_csv_all_type_no_partition(context).interim(
        "everythingSG_1",
        "Ylm8nxqQL0zBxirEscN9G$$J25jTrcz--p8rIEq91dDc",
        "AkcClQlLEjW2_hZLVHd_K$$SA-cROB8PXlFV8Wu7PKnF"
      )
    Lookup_1(context, df_src_csv_all_type_no_partition)
    val df_Deduplicate_1 = Deduplicate_1(context, in1).interim(
      "everythingSG_1",
      "DahWHsXlMAstD4rJY7F5f$$4tsCEAz71B_ircSFHyMRm",
      "Jjk3m71fE08G6q2zh26ZY$$c7vBF9LBXOj8Sp0l_6cyC"
    )
    val df_Script_1 = Script_1(context, df_Deduplicate_1).interim(
      "everythingSG_1",
      "kyIIQzKdsKTdGSBM1MatI$$XFyiNSlAxJ_ITVUwLEFdz",
      "YCc7Bg9MF6bugh1dQnsfq$$emAGCBCiAuXT_3xnR5lAu"
    )
    val df_recursive = recursive.apply(context, df_Script_1)
    val df_Reformat_2 = Reformat_2(context, in0).interim(
      "everythingSG_1",
      "5gS1dA5unfLKwMouiXrPl$$wzMEj9fBYADsqc9IfaBJG",
      "LLew197ZSyrWJ2fjA9C6x$$usoHZqM4EAM0SPdubW248"
    )
    val df_Filter_1 = Filter_1(context, df_Reformat_2).interim(
      "everythingSG_1",
      "C2RSpZOuPn5o7ClpAoKkL$$z8jlL8bh7VCruGjffHABY",
      "6vGXt22DPL9pliXyv085d$$WBiKdlYVQSR6QM_qVmgtR"
    )
    val df_OrderBy_1 = OrderBy_1(context, df_Filter_1).interim(
      "everythingSG_1",
      "TViIru5fN74BknMnGUILB$$nFpc7sghAOupmy8Frh6YX",
      "FRO7Stm4Ff7dh8Xa6fC0w$$G_QbxFH3Mdn5Dzz9jJzLS"
    )
    val df_Limit_1 = Limit_1(context, df_OrderBy_1).interim(
      "everythingSG_1",
      "OtvUWiajn3ngzmQg21k2W$$VkpeR_yh9Gn-pTN3pUXok",
      "NRcYDFEfMdGRJOkB42L4s$$D7cUafvXyKDnwX_XADMv6"
    )
    val df_WindowFunction_1 = WindowFunction_1(context, df_Limit_1).interim(
      "everythingSG_1",
      "p5W8WNrrAreMidcCzLVEd$$_r_i6NsxVyt5zwP0unJcd",
      "nmvAoqvzzBrF5cIxBRAI1$$v5iVyjx5k9-UOp6iJpsjC"
    )
    val df_SetOperation_1 =
      SetOperation_1(context, df_WindowFunction_1, df_WindowFunction_1).interim(
        "everythingSG_1",
        "yqN5K1GQqjsAJ-woKnnTV$$rJMWBOP30SaRWnHheIpYe",
        "v7x2C2MRazGWjKkQERttd$$2SSatSnowt4bednaZwF_0"
      )
    val df_SchemaTransform_1 =
      SchemaTransform_1(context, df_SetOperation_1).interim(
        "everythingSG_1",
        "oHIPsDtxs5tBaSZJlYkmf$$Ap_ZWvJmsaX1z7kCYw50-",
        "MjDnwoouFjkC0ZsN-GcRZ$$pAXcYJ820F1dStXDMIWqH"
      )
    val df_Join_1 =
      Join_1(context, df_SchemaTransform_1, df_SchemaTransform_1).interim(
        "everythingSG_1",
        "JtXx8wc198zl08maCaW5r$$eXaioWU6UFGn6VQfxo0Hz",
        "aEL_fDC7jPCTy4wXza8-v$$1fx3FR5WwtlLGCEFxMFqy"
      )
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_Join_1)
      (df_RowDistributor_1_out0_temp.interim(
         "everythingSG_1",
         "vXfvMmWewr5q2i3Q2SDW0$$VfRF7eccm7Xu9NAJBNvKk",
         "out0$$p2Sd3MEnceSSdz9012q3O"
       ),
       df_RowDistributor_1_out1_temp.interim(
         "everythingSG_1",
         "vXfvMmWewr5q2i3Q2SDW0$$VfRF7eccm7Xu9NAJBNvKk",
         "out1$$brppcRJ4OEoAWPesxLwAV"
       )
      )
    }
    val df_Reformat_1 =
      Reformat_1(context, df_src_csv_all_type_no_partition).interim(
        "everythingSG_1",
        "AIVuJSxH0tNFbgCQNW6BV$$6tF_fI9wf1_mkG0AG_xo6",
        "W3MRq-d9zKu8u1rJGpB55$$WjgM43hYdsgmV2JI7rdYQ"
      )
    withSubgraphName("everythingSG_1", context.spark) {
      withTargetId("dest_12345", context.spark) {
        dest_12345(context, df_Reformat_1)
      }
    }
    val df_Aggregate_1 = Aggregate_1(context, df_RowDistributor_1_out0).interim(
      "everythingSG_1",
      "fGZh2lPw1nEiCfltSox7C$$aAZENDNNcQpqFgCok32ve",
      "JlARlNTpqpXju3Yb5n7Jb$$dexkwVIzrWlt4LFbKox4F"
    )
    val df_OrderBy_2 = OrderBy_2(context, df_RowDistributor_1_out1).interim(
      "everythingSG_1",
      "aP_W-nV_sQ6nQPtwcXxv-$$dxDx2k7x6Z60CBYBpZxcN",
      "YM9zrAKAXA7H536kn3q38$$J9voViO87JwI2b7SDfQ9b"
    )
    val df_FlattenSchema_1 = FlattenSchema_1(context, df_Aggregate_1).interim(
      "everythingSG_1",
      "Cu5R1zR_Uh5tPZvowbyoB$$nHBtd7X0EXn4Txl5CchpD",
      "01HxfgL5Kj9sroiuQyzHJ$$BiCTgt30b8Zpn9oQaQZNq"
    )
    (df_Reformat_1, df_FlattenSchema_1, df_OrderBy_2, df_recursive)
  }

}
