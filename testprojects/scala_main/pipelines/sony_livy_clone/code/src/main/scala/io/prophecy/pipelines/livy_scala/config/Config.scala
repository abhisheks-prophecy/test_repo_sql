package io.prophecy.pipelines.livy_scala.config

import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import io.prophecy.pipelines.livy_scala.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  c_expr:    String = "%11%",
  c_st_expr: String = "concat(industry_code_ANZSIC, industry_name_ANZSIC)",
  c_string:  String = "this is default",
  c_int:     Int = 11
) extends ConfigBase