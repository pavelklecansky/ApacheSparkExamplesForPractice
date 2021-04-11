package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

/**
 * <h2> Graf úžasňáku </h2>
 * <b>Úkol:</b> Vytvořte graf rodiny úžasňáků.
 * Všechny vztahy mezi postavami nemusí být vytvořeni stačí pár.
 *
 * <b>Typ úkolu:</b> GraphX
 *
 * @author Pavel Klečanský
 *
 * */
object Incredibles {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "Incredibles")

    // Vytvoření pole vrcholů
    val people = Array((1L, ("Bob Parr", "Person")), (2L, ("Helen Parr", "Person")), (3L, ("Violet Parr", "Person")),
      (4L, ("Dash Parr", "Person")), (5L, ("Jack-Jack Parr", "Person")))

    // Vytvoření pole hran
    val relationships = Array(Edge(1L, 3L, "Father"), Edge(2L, 1L, "Wife"), Edge(4L, 3L, "Brother"), Edge(2L, 5L, "Mother"))

    // Převedení hran a vrcholu na RDD
    val characters = sc.parallelize(people)
    val incredibles_relationships = sc.parallelize(relationships)

    // Vytvoření defaultního vertexu
    val defaultvertex = ("Self", "Missing")

    // Vytvoření grafu
    val incredibles = Graph(characters, incredibles_relationships, defaultvertex)

    // Vypsání grafu do konsole
    for (person <- incredibles.triplets.collect) {
      print(person.srcAttr._1)
      print(" is ")
      print(person.attr)
      print(" with ")
      println(person.dstAttr._1)
    }
  }
}
