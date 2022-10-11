package com.main.sub_graph_src1.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.main.sub_graph_src1.config._
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
class Reformat_2Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Unit Test 0") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/com/main/sub_graph_src1/graph/Reformat_2/in/schema.json",
      "/data/com/main/sub_graph_src1/graph/Reformat_2/in/data/unit_test_0.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/com/main/sub_graph_src1/graph/Reformat_2/out/schema.json",
      "/data/com/main/sub_graph_src1/graph/Reformat_2/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed = com.main.sub_graph_src1.graph.Reformat_2(spark, dfIn)
    val res = assertDFEquals(dfOut.select("c- short", "c  - int"),
                             dfOutComputed.select("c- short", "c  - int"),
                             maxUnequalRowsToShow,
                             1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  test("Unit Test 1") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/com/main/sub_graph_src1/graph/Reformat_2/in/schema.json",
      "/data/com/main/sub_graph_src1/graph/Reformat_2/in/data/unit_test_1.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/com/main/sub_graph_src1/graph/Reformat_2/out/schema.json",
      "/data/com/main/sub_graph_src1/graph/Reformat_2/out/data/unit_test_1.json",
      "out"
    )

    val dfOutComputed = com.main.sub_graph_src1.graph.Reformat_2(spark, dfIn)
    val res = assertDFEquals(dfOut.select("c- short"),
                             dfOutComputed.select("c- short"),
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

    val dfMain_sub_graph_src1_graph_Subgraph_1_Lookup_1_1 =
      createDfFromResourceFiles(
        spark,
        "/data/com/main/sub_graph_src1/graph/Subgraph_1/Lookup_1_1/schema.json",
        "/data/com/main/sub_graph_src1/graph/Subgraph_1/Lookup_1_1/data.json",
        port = "in"
      )
    com.main.sub_graph_src1.graph.Subgraph_1
      .Lookup_1_1(spark, dfMain_sub_graph_src1_graph_Subgraph_1_Lookup_1_1)
  }

}
