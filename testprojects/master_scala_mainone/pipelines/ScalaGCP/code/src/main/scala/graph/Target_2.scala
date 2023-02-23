package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Target_2 {

  def apply(context: Context, in: DataFrame): Unit = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    in.write
      .format("jdbc")
      .option("url",     "jdbc:mysql://18.144.156.219:3306/test_database")
      .option("dbtable", "test_table_destination1")
      .option("user",
              dbutils.secrets.get(scope = "qasecrets_mysql", key = "username")
      )
      .option("password",
              dbutils.secrets.get(scope = "qasecrets_mysql", key = "password")
      )
      .option("driver", "com.mysql.jdbc.Driver")
      .mode("overwrite")
      .save()
  }

}
