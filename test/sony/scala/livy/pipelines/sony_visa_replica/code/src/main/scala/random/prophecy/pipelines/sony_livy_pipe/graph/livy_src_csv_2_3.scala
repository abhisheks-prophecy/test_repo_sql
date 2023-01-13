package random.prophecy.pipelines.sony_livy_pipe.graph

import io.prophecy.libs._
import random.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import random.prophecy.pipelines.sony_livy_pipe.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object livy_src_csv_2_3 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("year",                 StringType, true),
            StructField("lookup1",              StringType, true),
            StructField("industry_code_ANZSIC", StringType, true),
            StructField("industry_name_ANZSIC", StringType, true),
            StructField("rme_size_grp",         StringType, true),
            StructField("variable",             StringType, true),
            StructField("value",                StringType, true),
            StructField("unit",                 StringType, true),
            StructField("c_configs",            StringType, true)
          )
        )
      )
      .load("file:/storage/workflowdata/annual-enterprise/")

}
