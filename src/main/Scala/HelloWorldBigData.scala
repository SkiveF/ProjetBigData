import SparkBigDataTest.Session_Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.tools.nsc.doc.model.Public
import scala.collection.mutable._

/* class Bonjour {
  def main(args:Array[String]):Unit ={

  }
}*/

object HelloWorldBigData {
  /* premier programme Scala */
  val ma_var_imm: String = "skive" // variable immutable
  // table de hachage
  val states = Map(
    "AK" -> "Alaska",
    "HT" -> "HAITI",
    "KY" -> "kentucky"
  )
  val personne = Map(
    "nom" -> "FAMEUX",
    "prenom" -> "Skive",
    "age" -> 52
  )
  // tableaux ou array
  val arrayName: Array[String] = Array("skive", "SF", "509")
  private val une_var_imm: String = "Apprentissage formation Big Data" // variable à portée privée

  def main(args: Array[String]): Unit = {
    val sc = Session_Spark(true).sparkContext
    val session_s = Session_Spark(true)
    var test_mu: Int = 15 // variable mutable
    test_mu = test_mu + 10
    println("Hello World: Mon premier programme en Scala!!!")
    println(test_mu)
    val test_imm: Int = 25
    println("Votre texte contient : " + Comptage_caracteres("skive est là !!!!!!!!"))


    getResult("textttttte") // appel function getresult() dans la function main.
    testWhile(10)
    testFor()
    collectionScala()
    collectionTuple()

    val listOfNumbers: List[Int] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd: RDD[Int] = sc.parallelize(listOfNumbers)
    rdd.foreach(number => println(number + ","))

    listOfNumbers.zipWithIndex.foreach(print)

    import session_s.implicits._
    val df = sc.parallelize(listOfNumbers).toDF
    df.filter(row => Integer.parseInt(row.toString()) % 2 == 0).show()


  }

  // first function
  def Comptage_caracteres(texte: String): Int = {
    texte.trim.length()
  }

  //first procédure/méthode
  def getResult(parametre: Any): Unit = {
    if (parametre == 10) {
      println("votre valeur est un entier")
    } else {
      println("voptre texte n'est pas un entier")
    }
  }

  // structure conditionnelles
  def testWhile(value_to_test: Int): Unit = {
    var i: Int = 0
    while (i < value_to_test) {
      println(i)
      i = i + 1
    }
  }

  def testFor(): Unit = {
    var i: Int = 0
    for (i <- 5 to 15) {
      println("test iteration For " + i)
    }
  }

  //collection en scala
  def collectionScala(): Unit = {
    val maliste: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 12)
    val liste_S: List[String] = List("paul", "jean", "pierre", "julien")
    val plage_v: List[Int] = List.range(1, 15, 2)

    /*for (i <- liste_S){ 1
      print(i)
    }*/

    /* print(maliste(0))
      manupilation des collections à l'aide des fonctions anonymes
     on retourne ici le prenom terminant par n */
    val resultats: List[String] = liste_S.filter(e => e.endsWith("n"))
    for (x <- resultats) {
      println(x)
    }
    var res: Int = liste_S.count(i => i.endsWith("n"))
    println("nombre d'éléments respectant la condition :" + res)

    val maliste2: List[Int] = maliste.map(e => e * 2)
    for (f <- maliste2) {
      println(f)
    }
    val maliste3: List[Int] = maliste.map((e: Int) => e * 2)
    for (t <- maliste3) {
      println(t)
    }
    val maliste4: List[Int] = maliste.map(_ * 2)
    for (s <- maliste4) {
      println(s)
    }

    val nouvelle_liste: List[Int] = plage_v.filter(p => p > 5)
    nouvelle_liste.foreach(z => println("nouvelle liste " + z))
    val new_list: List[String] = liste_S.map(s => s.capitalize)
    new_list.foreach(e => println("nouvelle liste " + e))

  }

  //les tuples
  def collectionTuple(): Unit = {
    val tuple_test = (45, "sf", "false")
    println(tuple_test._1)

    val personne: Person = new Person("FAMEUX", "Skive", 10)
    val tuple_2 = ("Bonjour", personne, 54)
    tuple_2.toString().toList

  }

  // syntaxe 2
  def Comtage_caracteres1(texte: String): Int = {
    if (texte.isEmpty) {
      0
    } else {
      texte.trim.length()
    }
  }

  // syntaxe  3
  def comptage_caracteres2(texte: String): Int = texte.trim.length()

  class Person(var nom: String, var prenom: String, var age: Int)

  arrayName.foreach(e => println(e))


}
