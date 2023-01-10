package sonyprophecy.io_team.sony_scala_git

import org.apache.spark.sql._
package object functions {
  val udf1 = Udf1.udf1

  def registerFunctions(spark: SparkSession) =
    spark.udf.register("udf1", udf1)

}
