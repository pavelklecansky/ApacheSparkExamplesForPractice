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

  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
