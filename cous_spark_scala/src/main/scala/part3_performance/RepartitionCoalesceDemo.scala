package part3_performance

/**
 * =============================================================
 *   01 — Repartition vs Coalesce : partitionnement, shuffle
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object RepartitionCoalesceDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Repartition_Demo")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── État initial
    val nInit = df.rdd.getNumPartitions
    println(s"\nPartitions initiales : $nInit")

    // ── repartition()
    val dfUp = df.repartition(10)
    println(s"Après repartition(10)           : ${dfUp.rdd.getNumPartitions}")

    val dfDown = df.repartition(2)
    println(s"Après repartition(2)            : ${dfDown.rdd.getNumPartitions}")

    val dfByCol = df.repartition(4, col("country"))
    println(s"Après repartition(4, 'country') : ${dfByCol.rdd.getNumPartitions}")

    // ── coalesce()
    val dfCoal = df.repartition(10).coalesce(3)
    println(s"Après repartition(10)+coalesce(3): ${dfCoal.rdd.getNumPartitions}")

    // ── Distribution des données après repartition par colonne
    println("\n── Distribution par pays après repartition('country') ──")
    df.repartition(col("country")).groupBy("country").count().orderBy("country").show()

    // ── Cas concret : éviter les petits fichiers
    println("\n── Exemple écriture : coalesce avant write ──")
    val outputPath = s"$baseDir/output/coalesce_test"
    new File(s"$outputPath/employees_2parts").mkdirs()

    df.repartition(10).coalesce(2).write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/employees_2parts")
    println(s"Écrit avec 2 partitions dans : $outputPath/employees_2parts")

    println(
      """
        |Résumé :
        |  repartition(n)         → shuffle complet, montée ou descente, uniforme
        |  repartition(n, col)    → shuffle + hash par colonne, bon pour les jointures
        |  coalesce(n)            → pas de shuffle, descente uniquement, moins cher
        |""".stripMargin)

    spark.stop()
  }
}
