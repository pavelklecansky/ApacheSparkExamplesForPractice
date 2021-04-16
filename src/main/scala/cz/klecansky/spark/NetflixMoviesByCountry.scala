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

    // Vytvoření SparkSession
    val spark = SparkSession.builder().appName("NetflixMovies").master("local[*]").getOrCreate()

    // Načtení filmů jako dataframe
    val netflix = spark.read.option("header", true).option("inferSchema", true).csv("data/netflix_titles.csv")

    // Vytvoření dočasného view pro možnost použití SQL dotazů
    netflix.createOrReplaceTempView("netflix")

    // Sečtení filmů podle země výroby.
    val numberOfMoviesByCountry = spark.sql("SELECT country, count(*) as count from netflix GROUP BY country ORDER BY count DESC")

    // Zobrazení 20 zemí kde netflix vytvořil nejvíce pořadů
    numberOfMoviesByCountry.show()

    // Zastavení SparkSession
    spark.stop()
  }
}
