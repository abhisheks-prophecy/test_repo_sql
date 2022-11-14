package graph

import io.prophecy.libs._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      c1.as("c1"),
      when(
        col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) > lit(2),
        when(!(!(col("CMLS_DG_SETLMT_DT") =!= lit("00000000"))), lit("A"))
          .when(!(trim(trim(col("CMLS_REQST_MSG_SRCE_STN_ID"))) =!= lit("")),
                lit("B")
          )
          .otherwise(lit("X"))
      ).when(
          col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) <= lit(2),
          when(!(!(col("CMLS_DG_SETLMT_DT") =!= lit("00000000"))),    lit("C"))
            .when(!(!(col("CMLS_DG_SETLMT_DT") =!= lit("00000000"))), lit("D"))
            .otherwise(lit("Z"))
        )
        .otherwise(lit(null))
        .as("c2"),
      when(trim(trim(col("CMLS_REQST_MSG_SRCE_STN_ID"))) =!= lit(""),
           trim(col("CMLS_REQST_MSG_SRCE_STN_ID"))
      ).otherwise(lit(0)).as("c3"),
      when(col("CMLS_DG_SETLMT_DT") =!= lit("00000000"),
           col("CMLS_DG_SETLMT_DT")
      ).otherwise(lit("1900-01-01")).as("c4"),
      when(col("CMLS_DG_SETLMT_DT") =!= lit("00000000"),
           col("CMLS_DG_SETLMT_DT")
      ).otherwise(lit("1900-01-01")).as("c5"),
      coalesce(col("cmls_dest_bin"),        lit(0)).as("c6"),
      substring(col("CMLS_DG_PCR_ID_DRVD"), 1, 4).as("c7"),
      when(!(col("CMLS_DG_SETLMT_DT") =!= lit("00000000")),
           col("CMLS_DG_SETLMT_DT")
      ).otherwise(lit("1900-01-01")).as("c8"),
      when(!col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType).isNull,
           col("CMLS_DG_ISSR_NTWRK_ID")
      ).otherwise(lit(0)).as("c9")
    )

  val c1 = (col("CMLS_DG_ISSR_NTWRK_ID") === col("CMLS_DG_ISSR_NTWRK_ID"))
    .and(col("CMLS_DG_ISSR_NTWRK_ID") === col("CMLS_DG_ISSR_NTWRK_ID"))
    .and(
      (col("CMLS_DG_ISSR_NTWRK_ID") === col("CMLS_DG_ISSR_NTWRK_ID"))
        .and(col("CMLS_DG_ISSR_NTWRK_ID") === lit("s"))
    )
    .and(
      (col("CMLS_DG_ISSR_NTWRK_ID") === lit("w"))
        .and(col("CMLS_DG_ISSR_NTWRK_ID") === lit("s"))
        .and(col("CMLS_DG_ISSR_NTWRK_ID") === lit("w"))
    )
    .and(
      (col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) === lit(2001))
        .and(
          col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) === lit(2001) + lit(1)
        )
        .and(
          (col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) === lit(2001)).and(
            col("CMLS_DG_ISSR_NTWRK_ID")
              .cast(IntegerType) === lit(2001) + lit(1)
          )
        )
        .and(
          (col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) > lit(0))
            .and(col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) > lit(0))
            .and(
              when(col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) > lit(0),
                   col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) / col(
                     "CMLS_DG_ISSR_NTWRK_ID"
                   ).cast(IntegerType)
              ).otherwise(lit(null)) > when(
                col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) > lit(0),
                col("CMLS_DG_ISSR_NTWRK_ID").cast(IntegerType) / col(
                  "CMLS_DG_ISSR_NTWRK_ID"
                ).cast(IntegerType)
              ).otherwise(lit(null))
            )
        )
    )

}
