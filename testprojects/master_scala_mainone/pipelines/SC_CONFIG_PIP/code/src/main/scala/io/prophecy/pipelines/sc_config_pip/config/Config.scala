package io.prophecy.pipelines.sc_config_pip.config

import io.prophecy.pipelines.sc_config_pip.config.ConfigStore._
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  c_string:  String = "asdaslnjd#!@#%^&*()=-asd",
  c_int:     Int = -12313,
  c_float:   Float = 21312.123f,
  c_double:  Double = 9.07234123e8d,
  c_boolean: Boolean = true,
  c_short:   Short = 12,
  c_databricks_secret: DatabricksSecret =
    DatabricksSecret(scope = "qasecrets_mysql", key = "username"),
  c_spark_expression: String = "concat(`c  - int`, `c-string`)"
) extends ConfigBase

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
