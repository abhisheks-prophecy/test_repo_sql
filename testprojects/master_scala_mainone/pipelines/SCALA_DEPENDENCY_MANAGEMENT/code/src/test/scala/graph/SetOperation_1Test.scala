package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import config._
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class SetOperation_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test 0") {

    val dfIn0 = createDfFromResourceFiles(
      spark,
      "/data/graph/SetOperation_1/in0/schema.json",
      "/data/graph/SetOperation_1/in0/data/unit_test_0.json",
      "in0"
    )
    val dfIn1 = createDfFromResourceFiles(
      spark,
      "/data/graph/SetOperation_1/in1/schema.json",
      "/data/graph/SetOperation_1/in1/data/unit_test_0.json",
      "in1"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/graph/SetOperation_1/out/schema.json",
      "/data/graph/SetOperation_1/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed = graph.SetOperation_1(context, dfIn0, dfIn1)

  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

    val fabricName = System.getProperty("fabric")

    val config = ConfigurationFactoryImpl.fromCLI(
      Array("--confFile",
            getClass.getResource(s"/config/${fabricName}.json").getPath
      )
    )

    context = Context(spark, config)
  }

}
