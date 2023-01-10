package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph

case class Config(
  Subgraph_1: graph.Subgraph_1.config.Config = graph.Subgraph_1.config.Config()
) extends ConfigBase
