package io.prophecy.pipelines.scala_vector_type.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object target_scala_vector {

  def apply(context: Context, in: DataFrame): Unit = {
    import _root_.io.delta.tables._
    in.write
      .format("delta")
      .mode("overwrite")
      .save("dbfs:/tmp/e2e/target_scala_vector_1")
  }

}
