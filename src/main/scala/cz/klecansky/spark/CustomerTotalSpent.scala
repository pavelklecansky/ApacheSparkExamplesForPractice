package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._

/**
 * <h2> Zjištění celkové útraty zákazníku. </h2>
 * <b>Úkol:</b> Pomocí csv souboru customer-orders.csv
 * zjištěte celkovou útratu jednotlivých zákazníku.
 *
 * <b>Typ úkolu:</b> RDD
 *
 *
 * @author Pavel Klečanský
 * */
object CustomerTotalSpent {
  def main(args: Array[String]): Unit = {

    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "CustomerTotalSpent")

    // Vytvoření RDD z textového souboru.
    val data = sc.textFile("data/customer-orders.csv")

    // Vytvoření nového RDD ve formátu (id uživatele, částka)
    val orders = data.map(line => {
      val split = line.split(",")
      (split(0).toInt, split(2).toFloat)
    })

    // Sečtení částek podlé id uživatele.
    val customerTotalSpent = orders.reduceByKey((x, y) => x + y)

    // Převedení RDD na kolekci.
    val results = customerTotalSpent.collect()

    // Výpis do konzole
    results.foreach(println)
  }
}
