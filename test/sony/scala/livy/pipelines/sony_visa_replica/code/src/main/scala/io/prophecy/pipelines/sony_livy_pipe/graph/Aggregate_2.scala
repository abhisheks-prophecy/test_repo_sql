package io.prophecy.pipelines.sony_livy_pipe.graph

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Aggregate_2 {

  case class AggregateProperties(
    @Property("Last open tab") activeTab:          String = "aggregate",
    @Property("Columns selector") columnsSelector: List[String] = Nil,
    @Property("Group by expressions",
              "List of all the group by expressions"
    ) groupBy: List[SColumnExpression] = Nil,
    @Property("Aggregate expressions",
              "List of all the aggregate expressions"
    ) aggregate: List[SColumnExpression] = Nil,
    @Property("Do pivot",
              "Whether pivot should be performed"
    ) doPivot: Boolean = false,
    @Property("Pivot column",
              "Column to perform the pivot on"
    ) pivotColumn: Option[SColumn] = None,
    @Property("Aggregate expressions",
              "Column values to pivot on"
    ) pivotValues: List[StringColName] = Nil,
    @Property("flag to allow/disallow selection",
              "user will see 'clicking-ux' if this flag is true"
    ) allowSelection: Option[Boolean] = Some(true),
    @Property("Flag to propagate all columns",
              "Flag to propagate all columns"
    ) allIns: Option[Boolean] = Some(false)
  ) extends ComponentProperties

  @Property("String Wrapper") case class StringColName(
    @Property("Sort") colName: String
  )

  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark  = context.spark
    val Config = context.config
    val props = AggregateProperties(
      activeTab = "aggregate",
      columnsSelector = List(),
      groupBy = List(),
      aggregate = List(),
      doPivot = false,
      pivotColumn = None,
      pivotValues = List(),
      allowSelection = Some(true),
      allIns = Some(false)
    )
    val propagate_cols = props.allIns match {
      case None =>
        props.aggregate.tail.map(x => x.expression.column.as(x.target))
      case Some(value) =>
        if (value) {
          val agg_cols = props.aggregate.map(x => x.target)
          val groupBy_cols =
            if (props.doPivot)
              props.pivotColumn.get.column.toString() :: props.groupBy.map(x =>
                x.target
              )
            else
              props.groupBy.map(x => x.target)
          props.aggregate.tail.map(x =>
            x.expression.column.as(x.target)
          ) ++ in.columns.toList
            .diff(agg_cols ++ groupBy_cols)
            .map(x => first(col(x)).as(x))
        } else
          props.aggregate.tail.map(x => x.expression.column.as(x.target))
    }
    val out = props.groupBy match {
      case Nil =>
        in.agg(props.aggregate.head.expression.column
                 .as(props.aggregate.head.target),
               propagate_cols: _*
        )
      case _ =>
        val grouped = in.groupBy(
          props.groupBy.map(x => x.expression.column.as(x.target)): _*
        )
        val pivoted = if (props.doPivot) {
          (props.pivotValues.map(_.colName),
           props.pivotColumn.get.column
          ) match {
            case (Nil, column) =>
              grouped.pivot(column)
            case (nonNil, column) =>
              grouped.pivot(column, nonNil)
            case _ =>
              grouped
          }
        } else
          grouped
        pivoted.agg(props.aggregate.head.expression.column
                      .as(props.aggregate.head.target),
                    propagate_cols: _*
        )
    }
    out
  }

}
