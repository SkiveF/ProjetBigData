import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigDataTest._
import java.util._


object Spark_DB {

  def main(args: Array[String]): Unit = {
    val spark_s = Session_Spark()
    val props_mysql = new Properties()
    props_mysql.put("user", "SF_devs")
    props_mysql.put("password", "root1")

    val df_mysql = spark_s.read.jdbc("jdbc:mysql://localhost:3306/Spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC", "Spark_db.orders", props_mysql)

    val df_mysql2 = spark_s.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/Spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
      .option("user", "SF_devs")
      .option("password", "root1")
      .option("dbtable", "(select state,city,sum(round(numunits * totalprice)) as total_amount from orders group by state,city) table_summary")
      .load()


    val props_postgreSQL = new Properties()
    props_postgreSQL.put("user", "postgres")
    props_postgreSQL.put("password", "password")

    val df_postgresql = spark_s.read.jdbc("jdbc:postgresql://127.0.0.1:5432/Spark_db", "orders", props_postgreSQL)

    val df_postgresql2 = spark_s.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/Spark_db")
      .option("user", "postgres")
      .option("password", "password")
      .option("dbtable", "(select state,city,sum(round(numunits * totalprice)) as total_amount from orders group by state,city) table_postgresql")
      .load()

    val props_SQLServer = new Properties()
    props_SQLServer.put("user", "SF_devs")
    props_SQLServer.put("password", "root1")

    val df_sqlserver = spark_s.read.jdbc("jdbc:sqlserver://FAMEUX-PC\\SPARKSQLSERVER0:1433;databaseName=Spark_db;", "orders", props_SQLServer)
    val df_sqlServer2 = spark_s.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://FAMEUX-PC\\SPARKSQLSERVER0:1433;databaseName=Spark_db;integratedSecurity=true;")
      .option("dbtable", "(select state,city,sum(numunits * totalprice) as total_amount from orders group by state,city) table_postgresql")
      .load()


    df_sqlServer2.show(10)
  }

}
