package graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.Subgraph_4.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

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
  c_regex2:         String = "((?=.*)(?=.*[a-z$$])(?=.*[A-Z])(?=.*[@#%]).{6,20})",
  c_array_long:     List[Long] = List(10L),
  c_array_string:   List[String] = List("this is string1", "this is string2"),
  c_record_complex: C_record_complex = C_record_complex(),
  c_string_with_dollar: String =
    "mynameis$$iam$$anthony $$gonzales$$yes$$  $$$CONFIG_STR yes sir $$$$$$$c_sql_expr"
) extends ConfigBase

object C_record_complex {

  implicit val confHint: ProductHint[C_record_complex] =
    ProductHint[C_record_complex](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_record_complex(
  cr_array: List[Cr_array] = List(
    Cr_array(crar_short = 11, crar_long = 22L, crar_float = 33.33f)
  ),
  cr_record: Cr_record = Cr_record(),
  cr_string: String = "this is a string",
  cr_int:    Int = 11
)

object Cr_array {

  implicit val confHint: ProductHint[Cr_array] =
    ProductHint[Cr_array](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Cr_array(crar_short: Short, crar_long: Long, crar_float: Float)

object Cr_record {

  implicit val confHint: ProductHint[Cr_record] =
    ProductHint[Cr_record](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Cr_record(
  crr_array_bool: List[Boolean] = List(false, true),
  crr_array_spark_expression: List[String] = List(
    "concat(first_name, last_name)",
    "concat(first_name, first_name)",
    "concat(first_name, first_name, last_name)"
  )
)

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

case class Context(spark: SparkSession, config: Config)
