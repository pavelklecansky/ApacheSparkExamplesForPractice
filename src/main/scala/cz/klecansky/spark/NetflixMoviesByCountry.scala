package cz.klecansky.spark


import org.apache.log4j._
import org.apache.spark.sql._

/**
 * <h2> Netflix počet filmů pod zemí </h2>
 * <b>Úkol:</b> Pomocí csv souboru netflix_titles.csv
 * zjištěte počet filmů vytvořeních v jednotlivích zemích.
 * Použijte pro to SQL příkaz.
 *
 * <b>Typ úkolu:</b> SparkSQL
 *
 * @author Pavel Klečanský
 *
 * */
object NetflixMoviesByCountry {
  def main(args: Array[String]): Unit = {

    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
