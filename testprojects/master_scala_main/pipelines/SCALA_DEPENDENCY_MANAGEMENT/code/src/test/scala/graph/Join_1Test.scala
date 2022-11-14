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
class Join_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Unit Test 0") {

    val dfIn0 =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/in0/schema.json",
                                "/data/graph/Join_1/in0/data/unit_test_0.json",
                                "in0"
      )
    val dfIn1 =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/in1/schema.json",
                                "/data/graph/Join_1/in1/data/unit_test_0.json",
                                "in1"
      )
    val dfOut =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/out/schema.json",
                                "/data/graph/Join_1/out/data/unit_test_0.json",
                                "out"
      )

    val dfOutComputed = graph.Join_1(spark, dfIn0, dfIn1)
    val res = assertDFEquals(
      dfOut.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp"
      ),
      dfOutComputed.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  test("Unit Test 1") {

    val dfIn0 =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/in0/schema.json",
                                "/data/graph/Join_1/in0/data/unit_test_1.json",
                                "in0"
      )
    val dfIn1 =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/in1/schema.json",
                                "/data/graph/Join_1/in1/data/unit_test_1.json",
                                "in1"
      )
    val dfOut =
      createDfFromResourceFiles(spark,
                                "/data/graph/Join_1/out/schema.json",
                                "/data/graph/Join_1/out/data/unit_test_1.json",
                                "out"
      )

    val dfOutComputed = graph.Join_1(spark, dfIn0, dfIn1)
    val res = assertDFEquals(
      dfOut.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp"
      ),
      dfOutComputed.select(
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp",
        "c   short  --",
        "c-int-column type",
        "-- c-long",
        "c-decimal",
        "c  float",
        "c--boolean",
        "c- - -double",
        "c___-- string",
        "c  date",
        "c_timestamp"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

    val fabricName = System.getProperty("fabric")

    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(
      Array("--confFile",
            getClass.getResource(s"/config/${fabricName}.json").getPath
      )
    )
  }

}
