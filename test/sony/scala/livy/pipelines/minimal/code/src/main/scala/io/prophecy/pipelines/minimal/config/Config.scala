package io.prophecy.pipelines.minimal.config

import io.prophecy.pipelines.minimal.config.ConfigStore._
import io.prophecy.pipelines.minimal.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  c_string:  String = "this is test instance1",
  c_int:     Int = 11,
  c_expr:    String = "%11%",
  c_st_expr: String = "concat(industry_code_ANZSIC, industry_name_ANZSIC)"
) extends ConfigBase
