import org.apache.log4j.{LogManager, Logger}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.io.FileNotFoundException


object SparkBigData {

  // Developpement d'applications Big Data en Spark

  var ss: SparkSession = null
  // var spConf: SparkConf = null


  private var trace_log: Logger = LogManager.getLogger("logger_console")


  def main(args: Array[String]): Unit = {

    val test = Session_Spark(true)
    val sc = test.sparkContext
    sc.setLogLevel("OFF")
    val rdd_sf: RDD[String] = sc.parallelize(List("Alain", "skive", "julien", "jacques"))
    rdd_sf.foreach { l => println(l) }
    println("test")


    /* val rdd_test: RDD[String] = sc.parallelize(List("Alain", "skive", "julien", "jacques"))
     rdd_test.foreach(r => println(r))

     val rdd2: RDD[String] = sc.parallelize(Array("Lucie", "fabien", "jules"))
     rdd2.foreach (l=>println(l))*/

    /* val rdd3 = sc.parallelize(Seq(("Julien", "Math", 15), ("Aline", "Math", 17), ("Juvénal", "Math", 19)))
     println("Premier élément de mon RDD 3")
     rdd3.take(1).foreach (l => println(l))

     if (rdd3.isEmpty()) {
       println("le RDD est vide")
     } else {
       rdd3.foreach (println)
     }*/
  }


  /**
   * fonction qui inialise et instancie une session spark
   *
   * @param Env : c'est une variable qui indique l'environnement sur lequel notre app est deployer
   *            si Env = True, alors l'app est deployée en local, sinn , elle est déployeé sur un cluster
   */
  def Session_Spark(Env: Boolean = true): SparkSession = {
    try {
      if (Env == true) {
        System.setProperty("hadoop.home.dir", "C:/Hadoop")

        ss = SparkSession.builder()
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      } else {
        ss = SparkSession.builder()
          .appName("mon app Spark")
          .config("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      }
    } catch {

      case ex: FileNotFoundException => trace_log.error("Nous n'avaons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex: Exception => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
    }
    return ss
  }

}
