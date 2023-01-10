package io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1.Subgraph_2_1_2

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.sony_livy_pipe.config._
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
class Reformat_3_1_2Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test 0") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/in/schema.json",
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/in/data/unit_test_0.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/out/schema.json",
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1.Subgraph_2_1_2
        .Reformat_3_1_2(context, dfIn)
    val res = assertDFEquals(
      dfOut.select("year",
                   "industry_code_ANZSIC",
                   "industry_name_ANZSIC",
                   "rme_size_grp",
                   "variable",
                   "value",
                   "unit"
      ),
      dfOutComputed.select("year",
                           "industry_code_ANZSIC",
                           "industry_name_ANZSIC",
                           "rme_size_grp",
                           "variable",
                           "value",
                           "unit"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  test("Unit Test 1") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/in/schema.json",
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/in/data/unit_test_1.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/out/schema.json",
      "/data/io/prophecy/pipelines/sony_livy_pipe/graph/livyscalaSG1_1/Subgraph_2_1_2/Reformat_3_1_2/out/data/unit_test_1.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1.Subgraph_2_1_2
        .Reformat_3_1_2(context, dfIn)
    val res = assertDFEquals(
      dfOut.select("year",
                   "industry_code_ANZSIC",
                   "industry_name_ANZSIC",
                   "rme_size_grp",
                   "variable",
                   "value",
                   "unit"
      ),
      dfOutComputed.select("year",
                           "industry_code_ANZSIC",
                           "industry_name_ANZSIC",
                           "rme_size_grp",
                           "variable",
                           "value",
                           "unit"
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

    val dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_1_1 =
      createDfFromResourceFiles(
        spark,
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_1_1/schema.json",
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_1_1/data.json",
        port = "in"
      )
    io.prophecy.pipelines.sony_livy_pipe.graph.Lookup_1_1_1(
      context,
      dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_1_1
    )
    val dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_1 =
      createDfFromResourceFiles(
        spark,
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_1/schema.json",
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_1/data.json",
        port = "in"
      )
    io.prophecy.pipelines.sony_livy_pipe.graph
      .Lookup_1_1(context, dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_1)
    val dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1 =
      createDfFromResourceFiles(
        spark,
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1/schema.json",
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1/data.json",
        port = "in"
      )
    io.prophecy.pipelines.sony_livy_pipe.graph
      .Lookup_1(context, dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1)
    val dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_2 =
      createDfFromResourceFiles(
        spark,
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_2/schema.json",
        "/data/io/prophecy/pipelines/sony_livy_pipe/graph/Lookup_1_2/data.json",
        port = "in"
      )
    io.prophecy.pipelines.sony_livy_pipe.graph
      .Lookup_1_2(context, dfProphecy_pipelines_sony_livy_pipe_graph_Lookup_1_2)
  }

}
