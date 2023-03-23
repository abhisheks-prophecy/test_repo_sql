package random.prophecy.pipelines.sony_livy_pipe.graph

import io.prophecy.libs._
import random.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import random.prophecy.pipelines.sony_livy_pipe.config.Context
import random.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import random.prophecy.pipelines.sony_livy_pipe.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_4_2 {
  def apply(context: Context, in: DataFrame): DataFrame = in.filter(lit(true))
}
