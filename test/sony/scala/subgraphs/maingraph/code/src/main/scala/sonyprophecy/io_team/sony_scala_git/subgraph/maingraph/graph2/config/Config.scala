package sonyprophecy.io_team.sony_scala_git.subgraph.maingraph.graph2.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import sonyprophecy.io_team.sony_scala_git.subgraph.maingraph.graph2

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  graph2_var1:                   Option[String] = None,
  graph2_var2:                   Int = 23,
  added_from_inside_graph3_var3: Boolean = true,
  graph3:                        graph2.graph3.config.Config = graph2.graph3.config.Config(),
  graph2_newvar:                 Int = 12
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
