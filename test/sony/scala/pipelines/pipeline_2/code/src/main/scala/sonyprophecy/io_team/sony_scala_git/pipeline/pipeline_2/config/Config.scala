package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph

case class Config(
  maingraph_1: graph.maingraph_1.config.Config =
    graph.maingraph_1.config.Config()
) extends ConfigBase
