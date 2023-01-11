package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.subgraph.maingraph

case class Config(
  maingraph:     maingraph.config.Config = maingraph.config.Config(),
  pipeline_var1: String = "pipeline var default value"
) extends ConfigBase
