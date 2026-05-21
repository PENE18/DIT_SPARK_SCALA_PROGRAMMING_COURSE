package part1_basics

/**
 * =============================================================
 *   04 — Filtres : comparaisons, logique, isin, like, between
 * =============================================================
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FilterDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Filter_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)
    println("DataFrame source :"); df.show(5)

    // ── Comparaisons
    println("\n── Comparaisons ──")
    df.filter(col("age") > 40).select("name", "age", "country").show()

    // ── AND / OR / NOT
    println("\n── AND / OR / NOT ──")
    df.filter((col("salary") > 5000) && (col("country") === "France")).show()
    df.filter((col("country") === "France") || (col("country") === "Germany")).show()
    df.filter(!(col("department") === "HR")).show()

    // ── Piège opérateur precedence
    println("\n── Parenthèses (CORRECT) ──")
    // ✅ Correct — en Scala les priorités sont moins piégeuses, mais on garde les parenthèses
    df.filter((col("age") > 30) && (col("salary") > 4000)).show(5)

    // ── isin
    println("\n── isin / !isin ──")
    df.filter(col("country").isin("France", "Germany")).show()
    df.filter(!col("country").isin("France", "Germany")).show()

    // ── isNull / isNotNull
    println("\n── isNull / isNotNull ──")
    println(df.filter(col("salary").isNotNull).count())

    // ── between
    println("\n── between(30, 45) ──")
    df.filter(col("age").between(30, 45)).select("name", "age").show()

    // ── like / contains / startsWith
    println("\n── like / contains ──")
    df.filter(col("name").like("A%")).show()
    df.filter(col("name").contains("Martin")).show()

    // ── Filtre SQL
    println("\n── Filtre SQL style ──")
    df.createOrReplaceTempView("emp")
    spark.sql(
      """
        SELECT name, salary, country
        FROM emp
        WHERE salary BETWEEN 4000 AND 6000
          AND country IN ('France', 'UK')
      """
    ).show()

    spark.stop()
  }
}
