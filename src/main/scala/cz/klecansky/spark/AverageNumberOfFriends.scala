package cz.klecansky.spark


import org.apache.log4j._
import org.apache.spark._

/**
 * <h2> Průměrný počet přátel u jednotlivých věků </h2>
 * <b>Úkol:</b> Pomocí csv souboru fakefriends-noheader.csv
 * zjistěte průměrného počtu přátel u jednotlivých věků
 *
 *
 * <b>Typ úkolu:</b> RDD
 *
 * @author Pavel Klečanský
 * */
object AverageNumberOfFriends {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "FriendsAverageAge")

    // Vytvoření nového RDD ze souboru fakefriends-noheader.csv
    val friends = sc.textFile("data/fakefriends-noheader.csv")

    // Vytvoření RDD které obsahuje tuple (věk,počet přátel)
    val rdd = friends.map(line => {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age, numFriends)
    })

    // Vytvoření key-value pairu kde klíč bude věk a value bude tuple (celkový počet přátel, počet lidí)
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // Vydělení celkového počtu přátel počtem lidí
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Vypsání do konsole
    val results = averagesByAge.collect()
    results.sorted.foreach(println)
  }
}
