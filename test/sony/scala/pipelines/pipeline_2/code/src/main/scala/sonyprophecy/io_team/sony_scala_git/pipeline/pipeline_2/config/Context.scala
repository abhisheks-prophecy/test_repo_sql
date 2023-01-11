package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
