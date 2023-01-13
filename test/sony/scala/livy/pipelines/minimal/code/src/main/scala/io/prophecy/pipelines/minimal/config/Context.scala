package io.prophecy.pipelines.minimal.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
