package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/**
 * <h2> Oblíbenost filmů </h2>
 * <b>Úkol:</b> Pomocí csv souboru u.data
 * seřadit a vypsat filmy podle oblíbenosti. Použijte pomocné
 * metody na datasetu.
 *
 * <b>Typ úkolu:</b> SparkSQL
 *
 * @author Pavel Klečanský
 *
 * */
object PopularMovies {

  case class Movie(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Vytvoření SparkSession
    val spark = SparkSession.builder().appName("PopularMovies").master("local[*]").getOrCreate()

    // Vytvoření schematu pro u.data. Schéma je potřeba vytvořit z důvodu že soubor nemá hlavičku.
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // Načtení filmů jako dataset
    import spark.implicits._
    val movies = spark.read.option("sep", "\t").schema(moviesSchema).csv("data/ml-100k/u.data").as[Movie]

    // Seřazení všech filmů podle popularity.
    val topMovies = movies.groupBy("movieID").count().orderBy(desc("count"))

    // Zobrazení 20 nejoblíbenějších filmů
    topMovies.show(20)

    // Zastavení SparkSession
    spark.stop()

  }
}
