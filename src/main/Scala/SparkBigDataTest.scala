import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, lit, round, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.storage.StorageLevel

import java.io.FileNotFoundException

object SparkBigDataTest {
  var sf: SparkSession = null
  val schema_order = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("compaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("payementtype", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("numorderlines", IntegerType, true),
    StructField("numunits", IntegerType, true)
  ))

  private var trace_log: Logger = LogManager.getLogger("Logger_Console")
  val schema_test = StructType(Array(
    StructField("Id_Client", IntegerType, true),
    StructField("InvoiceNo", IntegerType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", DateType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", DoubleType, true),
    StructField("Country", StringType, true),
    StructField("InvoiceTimestamp", TimestampType, true)

  ))
  val valid_phoneUDF: UserDefinedFunction = udf { (phone_to_test: String) => valid_phone(phone_to_test: String) }
  var spConf: SparkConf = null

  def main(args: Array[String]): Unit = {
    val session_s = Session_Spark(true)
    session_s.udf.register("valid_phone", valid_phoneUDF)

    /**
     * Dataframe: est un "dataset" organisé en colonnes nommées. C'est en réalité l'abstraction d'un RDD, sous forme de table équivalente à la table dans une base de données relationnelle ou un dataframe dans Python/R.
     * Techniquement, un data frame est une collection distribuée de données assise sur l'intersection du RDD et du SQL.
     * En d'autres termes, c'est une abstraction de RDD qui permet de bénéficier à la fois des fonctionnalités des RDD et du moteur d'exécution SQL de Spark.
     * Les data frame peuvent être construits à partir d'une large varieté de sources de données, telles que :
     * les fichiers de données structurés (csv, txt, dat, etc), les fichiers non-structurés (json, xml, parquet, orc, etc..),
     * les tables Hive, les tables de bases de données relationnelles (MySQL, PostGre, etc...),
     * les SGBD NoSQL (HBase, cassandra, elasticsearch, etc.), et les RDD existants.
     */
    manipulation_rdd()

    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_test)
      .csv("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2010-12-06.csv")
    //df_test.show(15)


    val df_grp = session_s.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\*")
    //println("df_test count : " + df_test.count() + " df_grp count : " + df_grp.count())


    val df_grp2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2010-12-06.csv", "D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2011-01-20.csv")

    //println("df_grp count : " + df_grp.count() + " df_grp2 count : " + df_grp2.count())
    //df_grp2.show(7)
    //df_test.printSchema()
    val df_2 = df_test.select(
      col("InvoiceNo").cast(StringType),
      col("Id_Client"),
      col("stockCode").cast(IntegerType).alias("code_de_la_marchandise")
    )
    //modification le type de la colonne
    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("valeur_constante", lit(50))
      .withColumnRenamed("_c0", "Id_client")
      .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("Id_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("Created_dt", current_timestamp())
      .withColumn("reduction_test", when(col("total_amount") > 15, lit(3)).otherwise(lit(0)))
      .withColumn("reduction",
        when(col("total_amount") < 15, lit(0))
          .otherwise(when(col("total_amount").between(15, 20), lit(3))
            .otherwise(when(col("total_amount") < 15, lit(4)))))
      .withColumn("net_income", col("total_amount") - col("reduction"))


    val df_notreduced = df_3.filter(col("reduction") === lit(0) && col("Country").isin("United Kingdom", "France", "USA"))

    // df_notreduced.show(6)

    //jointures de dataframe
    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\orders.txt")
    val df_ordersGood = df_orders.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")

    //df_orders.show(5)
    //df_orders.printSchema()

    val df_products = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\product.txt")


    val df_orderlines = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\orderline.txt")

    // jointure des 3 fichiers de txt
    val df_joinOrders = df_orderlines.join(df_ordersGood, df_ordersGood.col("orderid") === df_orderlines.col("orderid"), Inner.sql)
      .join(df_products, df_products.col("productid") === df_orderlines.col("productid"), "inner")


    // union des fichiers csv
    val df_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2010-12-06.csv")


    val df_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2011-01-20.csv")


    val df_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("iferSchema", "true")
      .load("D:\\Spark_Scala_Formation\\Dataframe\\csv_file\\2011-12-08.csv")


    val df_unionFiles = df_fichier1.union(df_fichier2.union(df_fichier3))


    // Agrégats
    df_joinOrders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("city")
      .sum("total_amount").as("commandes totales")
    // .show()

    // opération de fenêtrage
    val wn_spec = Window.partitionBy(col("state"))
    val df_windows = df_joinOrders.withColumn("ventes_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(wn_spec))
      .select(col("orderlineid"), col("zipcode"), col("PRODUCTGROUPNAME"), col("ventes_dep").alias("ventes_par_département"))
    //.show(10)

    /* df_windows.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("D:\\Spark_Scala_Formation\\Dataframe\\Ecriture")

    //df_unionFiles.show(5)
     //df_unionFiles.printSchema()
    //print(df_fichier3.count() + " " + df_unionFiles.count()) */

    // manupilation des dates
    df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MMMM/yyyy hh:mm:ss"))
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_jour", window(col("orderdate"), "5 days"))
      .select(col("periode_jour"),
        col("periode_jour.start"),
        col("periode_jour.end")
      )


    // string to date / manipulation des dates et temps
    df_unionFiles.withColumn("InvoiceDate", col("InvoiceDate").cast(DateType))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
      .withColumn("Invoice_add_month", add_months(col("InvoiceDate"), 2))
      .withColumn("Invoice_add_date", date_add(col("InvoiceDate"), 30))
      .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 25))
      .withColumn("Invoice_date_diff", datediff(current_date(), col("InvoiceDate")))
      .withColumn("InvoiceDateQuarter", quarter(col("InvoiceDate")))
      .withColumn("InvoiceDate_Id", unix_timestamp(col("InvoiceDate")))
      .withColumn("InvoiceDate_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd-MM-yyyy"))

    // manipulation des textes  et des expression régulières
    df_products
      .withColumn("ProductGp", substring(col("PRODUCTGROUPNAME"), 2, 3))
      .withColumn("PRODUCTID", length(col("PRODUCTGROUPNAME")))
      .withColumn("concat_product", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
      //.where(regexp_extract(trim(col("PRODUCTID")),"[0-9]{5}",0)=== trim(col("PRODUCTID")))
      .where(!col("PRODUCTID").rlike("[0-9]{5}"))
    //.show(10)

    import session_s.implicits._
    val phone_list: DataFrame = List("070565364", "8794007834", "33070855032").toDF("phone_number")
    phone_list.withColumn("test_phone", valid_phoneUDF(col("phone_number")))
      .show()


    // hive
    df_joinOrders.createOrReplaceTempView("orders")
    val df_sql: DataFrame = session_s.sql("select state,city,sum(round(numunits * totalprice)) as total_amount from orders group by state,city")
    //df_sql.show()

    phone_list.createOrReplaceTempView("phone_table")
    session_s.sql("select valid_phone(phone_number) as valid_phone from phone_table").show()

    val df_hive = session_s.table("orders") // lire une table à partir du metastore Hive
    df_sql.write.mode(SaveMode.Overwrite).saveAsTable("report_orders") // enregistrer et écrire un data frame dans les metastore Hive


  }

  def valid_phone(phone_to_test: String): Boolean = {

    /**
     * UDF = user defined function sont une fonctionnalité qui permet d'étendre les capacités de Spark au delà des bibliothèques de classe Spark traditionnelles.
     * Elles vous permettent de rédiger vos propres routines/fonctions qui agirons individuellement sur chaque ligne d'un data frame.
     *
     */
    var result: Boolean = false
    var motif_regexp = "^0[0-9]{9}".r
    if (motif_regexp.findAllIn(phone_to_test.trim) == phone_to_test.trim) {
      result = true
    } else {
      result = false
    }
    return result
  }


  def spark_hdfs(): Unit = {

    // instanciation hdfs

    val config_fs = Session_Spark(env = true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_path = new Path("/user/datalake/marketing/")
    val dest_path = new Path("/user/datalake/indexes")
    val rename_src = new Path("/user/datalake/marketing/fichier_reporting.parquet")
    val des_src = new Path("/user/datalake/marketing/reporting.parquet")
    val local_path = new Path("D:\\Spark_Scala_Formation\\Dataframe\\Ecriture\\part-00000-d9af84a2-c8b0-4419-b1c0-3531a9e68bc0-c000.csv")
    val local_path1 = new Path("D:\\Spark_Scala_Formation\\Dataframe")

    // lecture des dichiers d'un dosssier
    val files_list = fs.listStatus(src_path)
    files_list.foreach(f => println(f.getPath))

    val files_list1 = fs.listStatus(src_path).map(x => x.getPath)
    for (i <- 1 to files_list1.length) {
      println(files_list1(i))
    }
    // renomage des fichiers
    fs.rename(rename_src, des_src)

    // supprimer des fichiers dans dossier
    fs.delete(des_src, true)

    // copie des fichiers
    fs.copyFromLocalFile(local_path, dest_path)
    fs.copyToLocalFile(dest_path, local_path1)

  }

  /**
   * fonction qui initialise et instancie une session spark
   *
   * @param env : c'est une variable qui indique l'environnement sur lequel notre application est déployée.
   *            Si env = True, alors l'appli est déployée en local, sinon, elle est déployée sur un cluster
   */
  def Session_Spark(env: Boolean = true): SparkSession = {
    try {
      if (env == true) {
        System.setProperty("hadoop.home.dir", "C:/Hadoop/")
        sf = SparkSession.builder
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      } else {
        sf = SparkSession.builder
          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }
    } catch {
      case ex: FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex: Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
    }

    return sf

  }

  def manipulation_rdd(): Unit = {
    /**
     * Le RDD est une "collection" d'éléments partitionné à travers les noeuds du cluster.
     * C'est une abstraction de collection données distribuée qui permet aux développeurs d'effectuer des calculs parallèles en mémoire sur un cluster de façon complètement tolérante aux pannes
     * Formellement, un RDD est une collection partitionnée d'objets (instance de classe)  accessible en lecture-seule.
     * Etant donné qu'une RDD est une abstraction, elle n'a pas d'existence réelle, dès lors elle doit être explicitement créée ou instanciée
     * à travers des opérations déterministes sur des fichiers de données existants ou sur d'autres instances de RDD.
     * Caractéristiques d'un RDD (identique à ceux d'une collection Scala)
     * - lazy
     * - immutable
     * - In-memory (en cache ou cacheable)
     *
     */
    val sc = Session_Spark(true).sparkContext
    val session_s = Session_Spark(true)

    sc.setLogLevel("OFF")

    val text: String = "if debugging is the process of removing software bugs, then programming must be the process of putting then in"
    val rdd_test_skill = sc.parallelize(text.replaceAll("""[\p{Punct}&&[^.]]""", "").split("\\s+"))
    //rdd_test_skill.zip(sc.parallelize(Seq(1))).reduceByKey(_+_).foreach(println)
    //rdd_test_skill.zipWithIndex.reduceByKey((a:String, b:String)=> a.equals(b))
    rdd_test_skill.distinct.foreach(println)


    val rdd_sf: RDD[String] = sc.parallelize(List("Alain", "skive", "julien", "jacques"))
    rdd_sf.foreach { l => println(l) }
    //println("test")

    val rdd2: RDD[String] = sc.parallelize(Array("Lucie", "fabien", "jules"))
    rdd2.foreach { l => println(l) }

    val rdd3 = sc.parallelize(Seq(("Julien", "Math", 15), ("Aline", "Math", 17), ("Juvénal", "Math", 19)))
    println("Premier élément de mon RDD 3")
    rdd3.take(1).foreach { l => println(l) }

    if (rdd3.isEmpty()) {
      println("le RDD est vide")
    } else {
      rdd3.foreach { l => println(l) }
    }
    //rdd3.saveAsTextFile("D:\\Spark_Scala_Formation\\rdd.txt")
    //rdd3.repartition(1).saveAsTextFile("D:\\Spark_Scala_Formation\\rdd3.txt")
    //rdd3.foreach{l => println(l)}
    //rdd3.collect().foreach{l => println(l)}


    // création d'un RDD à partir d'une source de donées
    val rdd4 = sc.textFile("D:/Spark_Scala_Formation/testtRDD.txt")
    println("Lecture du fichier RDD4")
    rdd4.foreach { l => println(l) }

    val rdd5 = sc.textFile("D:\\Spark_Scala_Formation\\*")
    println("Lecture du fichier RDD5")
    rdd5.foreach { l => println(l) }


    // transformation RDD
    val rdd_trans: RDD[String] = sc.parallelize(List("Alain mange une bannane", "la banane est un bon aliment ppur la santé", "acheter une banane bonne"))
    rdd_trans.foreach { l => println("ligne de mon rdd : " + l) }

    val rdd_map = rdd_trans.map(e => e.split(" "))
    println(" Nbr d'élements de mon rdd " + rdd_map.count())

    val rdd6 = rdd_trans.map(s => (s, s.length, s.contains("banane")))
    rdd6.foreach(l => println(l))

    val rdd7 = rdd6.map(x => (x._1.toUpperCase(), x._2, x._3))
    rdd7.foreach(l => print(l))

    val rdd8 = rdd6.map(x => (x._1.split(" "), 1))
    rdd8.foreach(l => println(l._1(0), l._2))

    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    rdd_fm.repartition(1).saveAsTextFile("D:Spark_Scala_Formation\\ComptageRDD.txt")
    rdd_fm.foreach(l => println(l))

    // affiche les phrase ayant le mot banane
    val rdd_flitered = rdd_fm.filter(x => x._1.contains("banane"))
    rdd_flitered.foreach(x => println(x))

    // compte les nombre de mot dans une phrase
    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y)
    rdd_reduced.foreach(l => println(l))

    rdd_fm.cache()
    //rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_fm.unpersist()

    // rdd tO DataFrame
    import session_s.implicits._
    val df: DataFrame = rdd_fm.toDF("texte", "valeur")
    //df.show(50)

  }


}
