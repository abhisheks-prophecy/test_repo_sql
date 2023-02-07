package org.main.scla_dep_mgmt.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.main.scla_dep_mgmt.config._
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
class Script_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test 0") {

    val dfIn2 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in2/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in2/data/unit_test_0.json",
      "in2"
    )
    val dfIn0 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in0/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in0/data/unit_test_0.json",
      "in0"
    )
    val dfIn3 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in3/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in3/data/unit_test_0.json",
      "in3"
    )
    val dfIn4 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in4/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in4/data/unit_test_0.json",
      "in4"
    )
    val dfIn5 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in5/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in5/data/unit_test_0.json",
      "in5"
    )
    val dfIn1 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in1/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/in1/data/unit_test_0.json",
      "in1"
    )
    val dfOut0 = createDfFromResourceFiles(
      spark,
      "/data/org/main/scla_dep_mgmt/graph/Script_1/out0/schema.json",
      "/data/org/main/scla_dep_mgmt/graph/Script_1/out0/data/unit_test_0.json",
      "out0"
    )

    val dfOut0Computed = org.main.scla_dep_mgmt.graph.Script_1(context,
                                                               dfIn0,
                                                               dfIn1,
                                                               dfIn2,
                                                               dfIn3,
                                                               dfIn4,
                                                               dfIn5
    )
    val res = assertDFEquals(
      dfOut0.select("customer_id",
                    "first_name",
                    "last_name",
                    "phone",
                    "email",
                    "country_code",
                    "account_open_date",
                    "account_flags"
      ),
      dfOut0Computed.select("customer_id",
                            "first_name",
                            "last_name",
                            "phone",
                            "email",
                            "country_code",
                            "account_open_date",
                            "account_flags"
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

    val config = ConfigurationFactoryImpl.fromCLI(
      Array("--confFile",
            getClass.getResource(s"/config/${fabricName}.json").getPath
      )
    )

    context = Context(spark, config)

    val dfMain_scla_dep_mgmt_graph_all_type_scala_sg_1_Lookup_1_1 =
      createDfFromResourceFiles(
        spark,
        "/data/org/main/scla_dep_mgmt/graph/all_type_scala_sg_1/Lookup_1_1/schema.json",
        "/data/org/main/scla_dep_mgmt/graph/all_type_scala_sg_1/Lookup_1_1/data.json",
        port = "in"
      )
    org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.Lookup_1_1(
      context,
      dfMain_scla_dep_mgmt_graph_all_type_scala_sg_1_Lookup_1_1
    )
  }

}
