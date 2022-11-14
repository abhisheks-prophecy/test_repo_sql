package io.prophecy.pipelines.scaladoanything.config

import io.prophecy.pipelines.scaladoanything.config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  c_boolean: Boolean = true,
  c_double:  Double = 1.01232132121e8d,
  c_float:   Float = 12312.123f,
  c_int:     Int = 185,
  c_long:    Long = 1054645645L,
  c_short:   Short = 12,
  c_db_secret: DatabricksSecret =
    DatabricksSecret(scope = "qasecrets_mysql", key = "username"),
  c_string1:                     String = "efefew wefewfwefew ewfwefewf wefewfewfewf. wefewf",
  c_expr_schematransform:        String = "concat(`c  - int`, `c- short`)",
  c_colname_schematransform:     String = "`c-date-for-today`",
  c_colnamedrop_schematransform: String = "c_double",
  c_regex_filter: String =
    "^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})",
  c_regex_filter2: String =
    "((?=.*\\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[@#%]).{6,20})",
  c_string:              Option[String] = None,
  c_to_delete:           String = "random_value",
  c_repartition_expr:    String = "concat(`c  float`, `c--boolean`)",
  c_repartition_colname: String = "`c  float`",
  c_sql_expr:            String = "in0.`c  - int` > 0 and in0.`c- short` > 1",
  c_sql_pattern:         String = "%[a-z]*%",
  c_agg_expr:            String = "first(`c  - int`)",
  c_date_for_today:      String = "`c_date-for today`",
  c_float_name:          String = "`c_float-__  `",
  c_row_number:          String = "row_number()",
  c_int_name:            String = "`c  - int`",
  c_join_condition:      String = "in0.`c  date`=in1.`c  date`",
  c_85:                  Int = 85,
  c_rd_expr:             String = "`c  float` < 5 and `c-int-column type` <= 85"
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
