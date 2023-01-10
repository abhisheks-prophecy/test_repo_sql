package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) =
    sonyprophecy.io_team.sony_scala_git.functions.registerFunctions(spark)

}
