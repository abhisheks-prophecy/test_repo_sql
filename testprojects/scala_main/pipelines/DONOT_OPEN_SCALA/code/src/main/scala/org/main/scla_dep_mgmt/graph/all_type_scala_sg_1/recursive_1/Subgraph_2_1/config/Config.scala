package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1.Subgraph_3_1.config.{
  Config => Subgraph_3_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(Subgraph_3_1: Subgraph_3_1_Config = Subgraph_3_1_Config())
    extends ConfigBase

case class Context(spark: SparkSession, config: Config)
