package io.prophecy.pipelines.scala_livy.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
