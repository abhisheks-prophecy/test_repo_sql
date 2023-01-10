package sonyprophecy.io_team.sony_scala_git.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Udf1 extends Serializable {
  var b    = 1000009
  def udf1 = udf((value: Int) => value * value * a)
}
