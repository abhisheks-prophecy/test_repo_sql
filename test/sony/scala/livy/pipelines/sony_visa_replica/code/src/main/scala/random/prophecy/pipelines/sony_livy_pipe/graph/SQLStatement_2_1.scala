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

object SQLStatement_2_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    in0.createOrReplaceTempView("in0")
    context.spark.sql("select * from in0")
  }

}
