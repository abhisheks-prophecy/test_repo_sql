package com.scala.main.job1.config

import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  @Description("test_str") c_test: Option[String] = None,
  @Description("array desc") c_array: List[String] =
    List("dasdsad", "sadasdsad", "yes sir", "2yes sir"),
  @Description("record ddesc") c_record3: C_record3 = C_record3(),
  @Description("bool desc") bool:         Boolean = true,
  @Description("double desc") double:     Double = 234324.0d
) extends ConfigBase

object C_record3 {

  implicit val confHint: ProductHint[C_record3] =
    ProductHint[C_record3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_record3(@Description("c value record") c_val3: C_val3 = C_val3())

object C_val3 {

  implicit val confHint: ProductHint[C_val3] =
    ProductHint[C_val3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_val3(@Description("crr desc") crr: String = "asdasdasd")
