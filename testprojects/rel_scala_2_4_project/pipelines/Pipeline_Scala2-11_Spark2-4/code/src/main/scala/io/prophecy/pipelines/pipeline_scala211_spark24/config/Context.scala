package io.prophecy.pipelines.pipeline_scala211_spark24.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
