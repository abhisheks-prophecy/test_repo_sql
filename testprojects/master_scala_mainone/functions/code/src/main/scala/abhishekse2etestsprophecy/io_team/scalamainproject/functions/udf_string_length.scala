package abhishekse2etestsprophecy.io_team.scalamainproject.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Udf_string_length extends Serializable {
  var int_value         = 10
  var string_value      = "string value"
  def udf_string_length = udf((s: String) => s.length)
}