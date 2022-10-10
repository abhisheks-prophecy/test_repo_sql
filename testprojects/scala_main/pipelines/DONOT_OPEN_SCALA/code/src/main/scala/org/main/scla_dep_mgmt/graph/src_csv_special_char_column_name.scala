package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object src_csv_special_char_column_name {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("c   short  --",     StringType, true),
            StructField("c-int-column type", StringType, true),
            StructField("-- c-long",         StringType, true),
            StructField("c-decimal",         StringType, true),
            StructField("c  float",          StringType, true),
            StructField("c--boolean",        StringType, true),
            StructField("c- - -double",      StringType, true),
            StructField("c___-- string",     StringType, true),
            StructField("c  date",           StringType, true),
            StructField("c_timestamp",       StringType, true)
          )
        )
      )
      .load("dbfs:/Prophecy/qa_data/csv/special_char_column_name")
      .cache()

}
