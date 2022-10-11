package io.prophecy.pipelines.livy_scala.config

import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  c_expr:    String = "%11%",
  c_st_expr: String = "concat(industry_code_ANZSIC, industry_name_ANZSIC)"
) extends ConfigBase
