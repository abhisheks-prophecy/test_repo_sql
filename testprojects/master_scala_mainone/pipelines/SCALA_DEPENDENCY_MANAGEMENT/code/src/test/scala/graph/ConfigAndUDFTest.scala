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
class ConfigAndUDFTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Unit Test 0") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/in/schema.json",
      "/data/graph/ConfigAndUDF/in/data/unit_test_0.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/out/schema.json",
      "/data/graph/ConfigAndUDF/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed = graph.ConfigAndUDF(spark, dfIn)
    val res = assertDFEquals(
      dfOut.select("customer_id",         "first_name", "last_name", "phone"),
      dfOutComputed.select("customer_id", "first_name", "last_name", "phone"),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  test("Unit Test 1") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/in/schema.json",
      "/data/graph/ConfigAndUDF/in/data/unit_test_1.json",
      "in"
    )

    val dfOutComputed = graph.ConfigAndUDF(spark, dfIn)

    assertPredicates(
      "out",
      dfOutComputed,
      Seq(
        col("customer_id") > lit(-1)
      ).zip(
        Seq(
          "p1"
        )
      )
    )
  }

  test("Unit Test 2") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/in/schema.json",
      "/data/graph/ConfigAndUDF/in/data/unit_test_2.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/out/schema.json",
      "/data/graph/ConfigAndUDF/out/data/unit_test_2.json",
      "out"
    )

    val dfOutComputed = graph.ConfigAndUDF(spark, dfIn)
    val res = assertDFEquals(
      dfOut.select(
        "customer_id",
        "first_name",
        "last_name",
        "phone",
        "email",
        "country_code",
        "account_open_date",
        "account_flags",
        "config_values",
        "udf_multiply_usage",
        "udf_string_null_safe_usage"
      ),
      dfOutComputed.select(
        "customer_id",
        "first_name",
        "last_name",
        "phone",
        "email",
        "country_code",
        "account_open_date",
        "account_flags",
        "config_values",
        "udf_multiply_usage",
        "udf_string_null_safe_usage"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  test("Unit Test 3") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/in/schema.json",
      "/data/graph/ConfigAndUDF/in/data/unit_test_3.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/graph/ConfigAndUDF/out/schema.json",
      "/data/graph/ConfigAndUDF/out/data/unit_test_3.json",
      "out"
    )

    val dfOutComputed = graph.ConfigAndUDF(spark, dfIn)
    val res = assertDFEquals(
      dfOut.select(
        "customer_id",
        "first_name",
        "last_name",
        "phone",
        "email",
        "country_code",
        "account_open_date",
        "account_flags",
        "config_values",
        "udf_multiply_usage",
        "udf_string_null_safe_usage"
      ),
      dfOutComputed.select(
        "customer_id",
        "first_name",
        "last_name",
        "phone",
        "email",
        "country_code",
        "account_open_date",
        "account_flags",
        "config_values",
        "udf_multiply_usage",
        "udf_string_null_safe_usage"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  def assertPredicates(
    port:       String,
    df:         DataFrame,
    predicates: Seq[(Column, String)]
  ): Unit = {
    predicates.foreach({
      case (pred, name) =>
        Assert.assertEquals(
          s"Predicate $name [[`$pred`]] not universally true for port $port",
          df.filter(pred).count(),
          df.count()
        )
    })
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
