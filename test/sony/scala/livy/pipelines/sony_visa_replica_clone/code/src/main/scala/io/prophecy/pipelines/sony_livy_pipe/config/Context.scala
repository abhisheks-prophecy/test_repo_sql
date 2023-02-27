package io.prophecy.pipelines.sony_livy_pipe.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
