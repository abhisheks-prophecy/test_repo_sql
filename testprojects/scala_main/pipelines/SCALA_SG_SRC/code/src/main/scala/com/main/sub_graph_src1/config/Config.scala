package com.main.sub_graph_src1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import com.main.sub_graph_src1.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}

case class Config(Subgraph_1: Subgraph_1_Config = Subgraph_1_Config())
    extends ConfigBase
