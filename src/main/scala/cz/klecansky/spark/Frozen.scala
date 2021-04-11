package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

/**
 * <h2> Modifikace grafu postav z ledového království </h2>
 * <b>Úkol:</b> Máte již vytvoření graf postav z ledového království a mezi nimi vytvořené základní vztahy.
 * Vaším úkolem je vytvořit nový modifikovaný graf který vychází z původního grafu.
 * <ul>
 * <li>Změntě Elsu z člověka na superhrdinu</li>
 * <li>Změntě vztah Anny k Hansovy na nepřátelé</li>
 * <li>Změntě vztah Anny k Kristoffovi na milenci</li>
 * </ul>
 *
 * <b>Typ úkolu:</b> GraphX
 *
 * @author Pavel Klečanský
 *
 * */
object Frozen {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "Incredibles")

    // Vytvoření pole vrcholů
    val people = Array((1L, ("Elsa", "Člověk")), (2L, ("Anna", "Člověk")), (3L, ("Kristoff", "Člověk")),
      (4L, ("Hans", "Člověk")))

    // Vytvoření pole hran
    val relationships = Array(Edge(1L, 2L, "Sestra"), Edge(2L, 4L, "Milenec"), Edge(2L, 3L, "Přítel"))

    // Převedení hran a vrcholu na RDD
    val characters = sc.parallelize(people)
    val frozen_relationships = sc.parallelize(relationships)

    // Vytvoření defaultního vertexu
    val defaultvertex = ("Self", "Missing")

    // Vytvoření grafu
    val frozen = Graph(characters, frozen_relationships, defaultvertex)
    // Vypsání grafu do konsole
    for (triplet <- frozen.triplets.collect) {
      print(triplet.srcAttr._1)
      print(" is a ")
      print(triplet.attr)
      print(" of ")
      println(triplet.dstAttr._1)
    }

    // --------------------------------------------
    // Kód nad tímto komentářem nijak nemodifikujte
    // --------------------------------------------

    // Změna vztahu mezi Annou a Hansem na nepřítelé
    var newFrozen = frozen.mapTriplets((triplet) => if (triplet.srcAttr._1 == "Anna" && triplet.dstAttr._1 == "Hans") "Nepřátelé" else triplet.attr)
    // Změna vztahu mezi Annou a Kristoffem na milenci
    newFrozen = newFrozen.mapTriplets((triplet) => if (triplet.srcAttr._1 == "Anna" && triplet.dstAttr._1 == "Kristoff") "Milenci" else triplet.attr)
    // Změnění Elsi z člověka na super hrdinu.
    newFrozen = newFrozen.mapVertices((id, user_type) => if (id == 1) ("Elsa", "Superhrdina") else user_type)

    // Vypsání nového grafu do konsole
    for (triplet <- newFrozen.triplets.collect) {
      print(triplet.srcAttr._1)
      print(" is a ")
      print(triplet.attr)
      print(" of ")
      println(triplet.dstAttr._1)
    }
  }
}