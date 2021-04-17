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

  }
}
