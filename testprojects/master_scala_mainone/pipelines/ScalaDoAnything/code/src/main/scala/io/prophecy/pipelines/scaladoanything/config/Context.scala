package io.prophecy.pipelines.scaladoanything.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
