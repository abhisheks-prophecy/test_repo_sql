package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_5 {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    // import epic.models.{ParserSelector}
    // val parser = epic.models.ParserSelector.loadParser("en")
    // print(parser)
    
    import cats.syntax.show._
    val shownInt = 123.show 
    print(shownInt)
    
    var out0=in0.select("c_array--long")
    out0
  }

}
