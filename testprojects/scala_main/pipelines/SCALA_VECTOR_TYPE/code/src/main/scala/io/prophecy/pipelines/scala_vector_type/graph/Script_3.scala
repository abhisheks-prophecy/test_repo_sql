package io.prophecy.pipelines.scala_vector_type.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.udfs._
import io.prophecy.pipelines.scala_vector_type.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_3 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.col
    
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")
    
    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")
    
    val key = Vectors.dense(1.0, 0.0)
    
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")
    
    val out0 = brp.fit(dfA).transform(dfA)
    out0
  }

}
