package config

import config.ConfigStore._
import config.Context
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
  @Description("new comment") CONFIG_BOOLEAN:    Boolean = true,
  @Description("new comment") CONFIG_DOUBLE:     Double = 123123.12321321d,
  @Description("new comment1") CONFIG_INT:       Int = 3243423,
  @Description("new comment") CONFIG_FLOAT:      Float = 3454.3455f,
  CONFIG_STR: String =
    "jdbc_url-jdbc:mysql://18.144.156.219:3306/test_database",
  c_0:         Int = 0,
  c_1:         Int = 1,
  c_st_expr:   String = "concat(`c_struct-c_short`, `c_struct-c_decimal`)",
  c_st_long:   String = "c_array_long",
  c_st_rename: String = "c_array_boolean_renamed",
  c_rd_expr:   String = "`c_decimal  -  ` >= 12321 ",
  c_limit_11:  Int = 11,
  c_row:       String = "row_number()",
  c_sql_expr:  String = "%1%",
  c_regex1: String =
    "^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})",
  c_regex2:       String = "((?=.*)(?=.*[a-z$$])(?=.*[A-Z])(?=.*[@#%]).{6,20})",
  c_array_long:   List[Long] = List(10L),
  c_array_string: List[String] = List("this is string1", "this is string2")
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
