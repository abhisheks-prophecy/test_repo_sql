package sonyprophecy.io_team.sony_scala_git.subgraph.maingraph.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import sonyprophecy.io_team.sony_scala_git.subgraph.maingraph

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  graph2: maingraph.graph2.config.Config = maingraph.graph2.config.Config(),
  @Description(
    "description added from maingraph config"
  ) maingraph_var1_edited_from_pipeline_config: Option[String] = None,
  maingraph_var2:                               Float = 33.0f,
  maingraph_var3:                               Option[String] = None,
  maingraph_newvar:                             String = "default var",
  maingraph_var4:                               String = "default var 4",
  newvar:                                       String = "asdf"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
