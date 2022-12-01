package config

import config.ConfigStore._
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  @Description("new comment") JDBC_USER:     String = "test_user",
  @Description("new comment2") SOURCE_TABLE: String = "test_table",
  @Description("new comment") db_secrets:    Option[DatabricksSecret] = None,
  @Description("new comment") JDBC_URL: String =
    "jdbc:mysql://18.144.156.219:3306/test_database",
  @Description("new comment") JDBC_SOURCE_TABLE: String = "test_table",
  @Description("new comment") CONFIG_STR:        String = "jdbc_url-${JDBC_URL}",
  @Description("new comment") CONFIG_BOOLEAN:    Boolean = true,
  @Description("new comment") CONFIG_DOUBLE:     Double = 123123.12321321d,
  @Description("new comment1") CONFIG_INT:       Int = 3243423,
  @Description("new comment") CONFIG_FLOAT:      Float = 3454.3455f
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
