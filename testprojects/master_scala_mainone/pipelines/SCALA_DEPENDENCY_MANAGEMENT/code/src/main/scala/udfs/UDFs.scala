package udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf_multiply",         udf_multiply)
    spark.udf.register("udf_string_null_safe", udf_string_null_safe)
    spark.udf.register("udf_string_length",    udf_string_length)
    spark.udf.register("new_udf",              new_udf)
  }

  def udf_multiply = {
    var int_value    = 10
    var string_value = "string value"
    udf((value: Int) => value * int_value)
  }

  def udf_string_null_safe = {
    var int_value    = 10
    var string_value = "string value"
    udf((s: String) => if (s != null) s.length else string_value.length)
  }

  def udf_string_length = {
    var int_value    = 10
    var string_value = "string value"
    udf((s: String) => s.length)
  }

  def new_udf = {
    var string_value = "string value"
    udf((s: String) => s.length)
  }

}

object PipelineInitCode extends Serializable {
  var int_value    = 10
  var string_value = "string value"
}
