package io.prophecy.pipelines.sc_config_pip.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
