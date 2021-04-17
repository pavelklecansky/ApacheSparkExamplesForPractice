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

  }
}
