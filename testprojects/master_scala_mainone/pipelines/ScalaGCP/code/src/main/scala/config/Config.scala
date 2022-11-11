package config

import config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  fabricName:                                    String,
  @Description("new comment") JDBC_USER:         String,
  @Description("new comment2") SOURCE_TABLE:     String,
  @Description("new comment") db_secrets:        Option[DatabricksSecret],
  @Description("new comment") JDBC_URL:          String,
  @Description("new comment") JDBC_SOURCE_TABLE: String,
  @Description("new comment") CONFIG_STR:        String,
  @Description("new comment") CONFIG_BOOLEAN:    Boolean,
  @Description("new comment") CONFIG_DOUBLE:     Double,
  @Description("new comment1") CONFIG_INT:       Int,
  @Description("new comment") CONFIG_FLOAT:      Float
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
