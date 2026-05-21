package part1_basics

/**
 * =============================================================
 *   01 — SparkSession : création, configuration, formats
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkSessionDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")

    // ── Création basique
    val spark = SparkSession.builder()
      .appName("SparkSession_Demo")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"Version Spark : ${spark.version}")
    println(s"SparkContext  : ${spark.sparkContext}")

    // ── Formats supportés par spark.read
    println("\nFormats supportés : CSV, JSON, Parquet, ORC, Text, JDBC, Delta")

    // ── Lire un CSV avec l'API générique format()
    val csvPath = s"$baseDir/data/csv/employees.csv"
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    println(s"\nLignes chargées : ${df.count()}")
    df.show(3)

    // ── Accès au SparkContext sous-jacent
    val sc = spark.sparkContext
    println(s"\nNom de l'application : ${sc.appName}")
    println(s"ID de l'application  : ${sc.applicationId}")

    spark.stop()
    println("\nDémonstration SparkSession terminée.")
  }
}
