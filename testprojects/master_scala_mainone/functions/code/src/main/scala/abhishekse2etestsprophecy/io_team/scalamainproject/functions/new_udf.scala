package abhishekse2etestsprophecy.io_team.scalamainproject.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object New_udf extends Serializable {
  var string_value = "string value"
  def new_udf      = udf((s: String) => s.length)
}
