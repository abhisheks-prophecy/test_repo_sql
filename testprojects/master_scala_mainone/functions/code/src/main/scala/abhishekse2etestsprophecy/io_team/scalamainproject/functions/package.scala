package abhishekse2etestsprophecy.io_team.scalamainproject

import org.apache.spark.sql._
package object functions {
  val udf_multiply         = Udf_multiply.udf_multiply
  val udf_string_null_safe = Udf_string_null_safe.udf_string_null_safe
  val udf_string_length    = Udf_string_length.udf_string_length
  val new_udf              = New_udf.new_udf

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("udf_multiply",         udf_multiply)
    spark.udf.register("udf_string_null_safe", udf_string_null_safe)
    spark.udf.register("udf_string_length",    udf_string_length)
    spark.udf.register("new_udf",              new_udf)
  }

}
