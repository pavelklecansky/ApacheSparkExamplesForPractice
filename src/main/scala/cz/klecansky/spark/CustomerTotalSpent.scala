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

  }
}
