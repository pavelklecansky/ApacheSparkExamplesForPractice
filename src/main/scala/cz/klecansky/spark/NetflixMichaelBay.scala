package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * <h2> Průměrná délka filmů Michaela Baye na Netflixu </h2>
 * <b>Úkol:</b> Pomocí csv souboru netflix_titles.csv
 * Zjistěte jaká je průměrná délka filmu Michaela Baye které jsou na Netflixu.
 *
 * <b>Typ úkolu:</b> SparkSQL
 *
 * @author Pavel Klečanský
 *
 * */
object NetflixMichaelBay {

  // Vytvoření třídy Movie
  case class Movie(show_id: String, show_type: String, title: String, director: String, cast: String, country: String, date_added: String, release_year: String, rating: String, duration: String, listed_in: String, description: String)

  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření SparkSession
    val spark = SparkSession.builder().appName("NetflixMovies").master("local[*]").getOrCreate()

    // Načtení filmů jako dataset
    import spark.implicits._
    val netflix = spark.read.option("header", true).option("inferSchema", true).csv("data/netflix_titles.csv").as[Movie]

    // Filtrovat pouze filmy které režíroval Michael Bay
    val michaelBayMovies = netflix.filter(movie => movie.director == "Michael Bay")

    // Vrátit délku jednotlivých filmů jako číslo
    val michaelBayMoviesDuration = michaelBayMovies.map(movie => movie.duration.replaceAll(" min", "").toInt)

    // Zjištění průměrné délky filmů
    val averageDuration = michaelBayMoviesDuration.select(avg("value"))

    // Vypsání do konsole
    println(averageDuration.first())
  }
}
