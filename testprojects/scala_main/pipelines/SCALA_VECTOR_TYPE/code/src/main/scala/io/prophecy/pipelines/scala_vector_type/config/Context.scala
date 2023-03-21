package io.prophecy.pipelines.scala_vector_type.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
