package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._


/**
 * <h2> Množství hodnocení </h2>
 * <b>Úkol:</b> Pomocí csv souboru u.data
 * zjistit množství u jednotlivých hodnocení.
 *
 * <b>Typ úkolu:</b> RDD
 *
 * @author Pavel Klečanský
 */
object CountRatings {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "CountRatings")

    // Vytvoření RDD z textového souboru.
    val data = sc.textFile("data/ml-100k/u.data")

    // Vytvoření nového RDD hodnocení
    val ratings = data.map(x => {
      val split = x.split("\t")
      split(2)
    })

    // Vytvoření RDD počtu jednotlivých hodnocení ve formátu (hodnota hodnocení, počet hodnocení)
    val results = ratings.countByValue()

    // Vrátit sequance a seřezení podle hodnocení
    val sortedResults = results.toSeq.sortBy(_._1);

    // Výpis do konzole
    sortedResults.foreach(println)


  }
}
