

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.FileNotFoundException

object SparkBigDataTest {
  var sf: SparkSession = null
  private var trace_log: Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {

    val sc = Session_Spark(true).sparkContext
    val session_s = Session_Spark(true)

    sc.setLogLevel("OFF")

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
    /* val rdd4 = sc.textFile("D:\\Spark_Scala_Formation\\testtRDD.txt")
     println("Lecture du fichier RDD4")
     rdd4.foreach{l=>println(l)}

     val rdd5 =sc.textFile("D:\\Spark_Scala_Formation\\*")
     println("Lecture du fichier RDD5")
     rdd5.foreach{l => println(l)}*/


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
    rdd_fm.foreach(l => println(l))

    val rdd_flitered = rdd_fm.filter(x => x._1.contains("banane"))
    rdd_flitered.foreach(x => println(x))

    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y)
    rdd_reduced.foreach(l => println(l))

    rdd_fm.cache()
    //rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_fm.unpersist()

    // rdd tO DataFrame
    import session_s.implicits._
    val df: DataFrame = rdd_fm.toDF("texte", "valeur")
    df.show(50)


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
          //    .enableHiveSupport()
          .getOrCreate()
      } else {
        sf = SparkSession.builder
          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      }
    } catch {
      case ex: FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex: Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
    }

    return sf

  }


}
