package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType, StringType}

/**
 * <h2> Oblíbenost filmů </h2>
 * <b>Úkol:</b> Pomocí csv souboru u.data
 * seřadit a vypsat filmy podle oblíbenosti. Použijte pomocné
 * metody na dataframy.
 *
 * <b>Typ úkolu:</b> SparkSQL
 *
 * @author Pavel Klečanský
 *
 * */
object PopularMovies {

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

    // Vytvoření redukovaného schematu pro u.item.
    val moviesMetadataSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieName", StringType, nullable = true)

    // Načtení filmů
    var movies = spark.read.option("sep", "\t").schema(moviesSchema).csv("data/ml-100k/u.data")
    // Načtení metadat k filmům
    val moviesMetaData = spark.read.option("sep", "|").schema(moviesMetadataSchema).csv("data/ml-100k/u.item")

    // Spojení filmy a meta data k filmům
    movies = movies.join(moviesMetaData, "movieID")

    // Seřazení všech filmů podle popularity.
    val topMovies = movies.groupBy("movieName").count().orderBy(desc("count"))

    // Zobrazení 20 nejoblíbenějších filmů
    topMovies.show(20)

    // Zastavení SparkSession
    spark.stop()
  }
}
