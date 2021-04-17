package cz.klecansky.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

/**
 * <h2> Graf úžasňáku </h2>
 * <b>Úkol:</b> Vytvořte graf rodiny úžasňáků.
 * Všechny vztahy mezi postavami nemusí být vytvořeni stačí pár.
 *
 * <b>Typ úkolu:</b> GraphX
 *
 * @author Pavel Klečanský
 *
 * */
object Incredibles {
  def main(args: Array[String]): Unit = {
    // Změna na error jinak Spark při spuštění vypisuje obrovské množství info logů.
    Logger.getLogger("org").setLevel(Level.ERROR)

  }
}
