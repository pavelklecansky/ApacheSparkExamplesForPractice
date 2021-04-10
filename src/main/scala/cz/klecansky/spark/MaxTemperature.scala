package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/**
 * <h2> Zjištění maximální naměřené teploty u jednotlivých statnic za druhé světové války </h2>
 * <b>Úkol:</b> Pomocí csv souboru weather-ww2.csv
 * zjistit jaké maxímální teploty naměřili jednotlivé stanice
 * během druhé světové války.
 *
 * <b>Typ úkolu:</b> RDD
 *
 * @author Pavel Klečanský
 * */
object MaxTemperature {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření nového Spark contextu
    val sc = new SparkContext("local[*]", "CustomerTotalSpent")

    // Vytvoření RDD z textového souboru.
    val data = sc.textFile("data/weather-ww2.csv")

    // Uložení hlavičky pro pozdější filtraci.
    val header = data.first()

    // Vytvoření nového RDD bez hlavičky
    // Vytvoření nového RDD ve formátu (označení stanice, maximální naměřená hodnota v určitém dnu)
    val weatherFromWW = data.filter(row => row != header).map(line => {
      val split = line.split(",")
      val station = split(0)
      val temperature = split(4).toFloat
      (station, temperature)
    })

    // Vypočet maximální teploty pro jednotlivé stanice.
    val maxTempsByStation = weatherFromWW.reduceByKey((x, y) => max(x, y))

    // Převedení RDD na kolekci.
    val results = maxTempsByStation.collect()

    // Výpis do konzole
    results.foreach(println)
  }
}
