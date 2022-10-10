package org.main.scla_dep_mgmt.config

import org.main.scla_dep_mgmt.config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  JDBC_USER:           String = "test_user",
  SOURCE_TABLE:        String = "test_table",
  db_secrets:          Option[DatabricksSecret] = None,
  JDBC_URL:            String = "jdbc:mysql://18.144.156.219:3306/test_database",
  JDBC_SOURCE_TABLE:   String = "test_table",
  CONFIG_STR:          String = "jdbc_url-${JDBC_URL}",
  CONFIG_BOOLEAN:      Boolean = true,
  CONFIG_DOUBLE:       Double = 123123.12321321d,
  CONFIG_INT:          Int = 3243423,
  CONFIG_FLOAT:        Float = 3454.3455f,
  c_limit_11:          Int = 11,
  c_st_expr:           String = "concat(`c_struct-c_short`, `c_struct-c_decimal`)",
  c_st_long:           String = "c_array_long",
  c_st_rename:         String = "c_array_boolean_renamed",
  c_dedup_expr:        String = "concat(c_array_float, `c_array_int`)",
  c_dedup_col:         String = " c_array_date",
  c_rd_expr:           String = "`c -  boolean _  ` in (true,false)",
  c_12321:             Int = 12321,
  c_0:                 Int = 0,
  c_1:                 Int = 1,
  c_join_expr:         String = "in0.`-- c-long`=in1.`-- c-long`",
  c_join_cshort:       String = "in0.`c   short  --`",
  c_orderby_expr:      String = "concat(`c  date`, c_timestamp)",
  c_orderby_int:       String = "`c-int-column type`",
  c_filter_expression: String = "customer_id >5",
  c_reformat_complex: String =
    "case     when c_int > 10 then         case              when NOT (NOT (c_string like '%1%')) then 'A'             when NOT (NOT (trim(trim(c_string)) = '')) then 'B'             else 'X'         end     when c_int <= 10 then         case             when NOT (NOT (c_string like '%1%')) then 'C'             when NOT (NOT (c_string not like '%2%')) then 'D'             else 'Z'         end     else null end",
  c_repartition_colname: String = "`c_float-__  `",
  c_repartition_expr: String =
    "concat(`c  - int`, `c_struct -- _  `.`c_string - of a struct -- _`)",
  c_agg_expr:  String = "first(c1)",
  c_agg_group: String = "concat(c1, c2, c3)",
  c_agg_c3:    String = "c3",
  c_row:       String = "row_number()",
  c_bool:      String = "`c -  boolean _  `",
  c_short:     String = "`c- short`",
  c_sql_expr:  String = "%[^aeiou]@%",
  c_sql_c8c1:  Int = -1,
  c_regex1: String =
    "^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})",
  c_regex2: String = "((?=.*)(?=.*[a-z$$])(?=.*[A-Z])(?=.*[@#%]).{6,20})",
  c_str:    String = "stringwith$$one#%^&*()-=!@#",
  c_new_sqlexpr: String =
    "select * from in0 where cast(SUBSTRING(in0.c9_udf1_c2, 1,2) as int) > -1"
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
