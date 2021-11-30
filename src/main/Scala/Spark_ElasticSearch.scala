import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import SparkBigDataTest._


object Spark_ElasticSearch {


 def main(args: Array[String]): Unit = {
  val session_s = Session_Spark(true)

  val df_orders = session_s.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .option("header", "true")
    .option("iferSchema", "true")
    .load("D:\\Spark_Scala_Formation\\Dataframe\\orders.txt")

  val df_games = session_s.read
    .format("com.databricks.spark.csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("iferSchema", "true")
    .load("D:\\Spark_Scala_Formation\\Dataframe\\games.csv")

  val df_games_ = df_games.withColumnRenamed("_c0", "id")


  /**
   * Ecriture des données du dataframe dans un index d'ElasticSearch
   */

  df_games_.write
    .mode(SaveMode.Append)
    .format("org.elasticsearch.spark.sql")
    .option("es.port", "9200")
    .option("es.nodes", "localhost")
    .save("index_nba_players/doc")

  /**
   * l'autre façon de faire
   *
   */
  val ss = SparkSession.builder
    .appName("Mon application Spark")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("es.port", "9200")
    .config("es.nodes", "localhost")
  df_games.saveToEs("index_games/doc")

 }
}
