import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigDataTest._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._


object Spark_Cassandra {

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)
    ss.conf.set(s"ss.sql.catalog.sf", "com.datastax.spark.connector.datasource.CassandraCatalog")
    ss.conf.set(s"spark.sql.catalog.sf.spark.cassandra.connection.host", "127.0.0.1")

    ss.sparkContext.cassandraTable("demo", "spacecraft_journey_catalog")

    val df_cassandra = ss.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "demo", "table" -> "spacecraft_journey_catalog", "cluster" -> "journey_id"))
      .load()

    val df_cassandra2 = ss.read
      .cassandraFormat("spacecraft_journey_catalog", "demo", "journey_id")
      .load()

    df_cassandra.printSchema()
    df_cassandra.explain()
    df_cassandra.show()

  }


}
