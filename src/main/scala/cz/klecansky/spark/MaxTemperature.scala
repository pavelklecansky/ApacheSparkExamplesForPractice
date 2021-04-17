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

  }
}
