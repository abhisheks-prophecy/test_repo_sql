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

object target_dataset {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save("file:/storage/workflowdata/out/target_123")

}
