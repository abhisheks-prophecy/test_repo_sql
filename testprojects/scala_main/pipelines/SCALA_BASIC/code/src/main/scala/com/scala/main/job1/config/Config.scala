package com.scala.main.job1.config

import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  c_test:    Option[DatabricksSecret] = None,
  c_array:   Float = 32432.0f,
  c_record3: C_record3 = C_record3()
) extends ConfigBase

object C_record3 {

  implicit val confHint: ProductHint[C_record3] =
    ProductHint[C_record3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_record3(c_val3: C_val3 = C_val3())

object C_val3 {

  implicit val confHint: ProductHint[C_val3] =
    ProductHint[C_val3](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_val3(crr: String = "asdasdasd")

object DatabricksSecret {

  implicit val myIntReader: ConfigReader[DatabricksSecret] =
    ConfigReader[String].map { s =>
      val Array(scope, key) = s.split(":")
      DatabricksSecret(scope, key)
    }

}

case class DatabricksSecret(scope: String, key: String) {

  override def toString: String = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    dbutils.secrets.get(scope = scope, key = key)
  }

}
