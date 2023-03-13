package com.main.sub_graph_src1.graph.Subgraph_1.recursive_1.Subgraph_2_1.Subgraph_3_1.Subgraph_4_1.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config() extends ConfigBase
case class Context(spark: SparkSession, config: Config)
