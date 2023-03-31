package io.prophecy.pipelines.scala_pipeline.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}
import io.prophecy.pipelines.scala_pipeline.graph.SCALA_SG.config.{
  Config => SCALA_SG_Config
}

case class Config(
  Subgraph_1: Subgraph_1_Config = Subgraph_1_Config(),
  SCALA_SG:   SCALA_SG_Config = SCALA_SG_Config()
) extends ConfigBase
