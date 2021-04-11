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

    // Vytvoření SparkSession
    val spark = SparkSession.builder().appName("PopularMovies").master("local[*]").getOrCreate()

    // Načtení knih jako dataframe
    val books = spark.read.option("header", true).option("inferSchema", true).csv("data/books.csv")

    // Sečtení nejproduktivnějších autorů s průmerným hodnocení knih nad 4.5
    val autherCount = books.filter(books("average_rating") > 4.5).groupBy("authors").count().orderBy(desc("count"))

    // Zobrazení 50 nejproduktivnějších autorů
    autherCount.show(50)

    // Zastavení SparkSession
    spark.stop()
  }
}
