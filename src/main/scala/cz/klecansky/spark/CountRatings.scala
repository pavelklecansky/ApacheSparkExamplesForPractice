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

  }
}
