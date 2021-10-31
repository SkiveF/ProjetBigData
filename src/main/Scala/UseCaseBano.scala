import SparkBigDataTest._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

object UseCaseBano {
  val schema_bano = StructType(Array(
    StructField("Id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)

  ))
  val config = new Configuration()
  val fs = FileSystem.get(config)
  val chemin_dest = new Path("D:\\Spark_Scala_Formation\\Dataframe\\Ecriture")

  def main(args: Array[String]): Unit = {
    val ss_s = Session_Spark(true)


    val df_bano_brute = ss_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_bano)
      .csv("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\full.csv")

    //df_bano_brute.show(10)
    // otherwise permet de faire la transformation d'un colonne
    val df_bano = df_bano_brute
      .withColumn("code_departement", substring(col("code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreetMap"))
        .otherwise(when(col("code_source_bano") === lit("OD"), lit("OpenData"))
          .otherwise(when(col("code_source_bano") === lit("O+O"), lit("OpenData OSM"))
            .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
              .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM")))))))

    //df_bano.show(30)

    val df_departement = df_bano.select(col("code_departement")).distinct().filter(col("code_departement").isNotNull)

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x => x(0)).toList

    // scala / une process
    liste_departement.foreach {
      x =>
        df_bano.filter(col("code_departement") === x.toString)
          .coalesce(1) // execution sur le noeud principale
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\bano" + x.toString)

        val chemin_source = new Path("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\bano" + x.toString)
        fs.copyFromLocalFile(chemin_source, chemin_dest)
    }

    // spark / multi process
    /** df_departement.foreach{
     * dep =>df_bano.filter(col("code_departement")=== dep.toString())
     * .repartition(1) // execution sur le noeud principale
     * .write
     * .format("com.databricks.spark.csv")
     * .option("delimiter",";")
     * .option("header","true")
     * .mode(SaveMode.Overwrite)
     * .csv("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\bano" + dep.toString)
     *
     * val chemin_source = new Path("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\bano" + dep.toString)
     *
     * fs.copyFromLocalFile(chemin_source,chemin_dest)
     * } */


  }


}
