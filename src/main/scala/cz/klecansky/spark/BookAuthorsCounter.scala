package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc

/**
 * <h2> Nejproduktivnější autoři s průmerným hodnocením knih nad 4.5 </h2>
 * <b>Úkol:</b> Pomocí csv souboru books.csv
 * Zjištět neproduktivnější autory s průměrným hodnocením knih nad 4.5
 *
 * <b>Typ úkolu:</b> SparkSQL
 *
 * @author Pavel Klečanský
 *
 * */
object BookAuthorsCounter {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
