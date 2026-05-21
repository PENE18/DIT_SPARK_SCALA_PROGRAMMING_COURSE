package part3_performance

/**
 * =============================================================
 *   03 — Catalyst Optimizer : explain, predicate pushdown,
 *        column pruning, constant folding
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CatalystExplainDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Catalyst_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── Plan physique de base
    println("\n── explain() — Plan physique ──")
    df.filter(col("country") === "France")
      .select("name", "salary", "country")
      .explain()

    // ── Plan formaté (Spark 3.0+)
    println("\n── explain('formatted') ──")
    df.filter(col("country") === "France")
      .select("name", "salary")
      .explain("formatted")

    // ── Predicate Pushdown : Catalyst déplace le filtre vers la source
    println("\n── Predicate Pushdown ──")
    println("Ces deux requêtes produisent le MÊME plan :")
    val q1 = df.filter(col("age") > 30).select("name", "age")
    val q2 = df.select("name", "age").filter(col("age") > 30)

    println("Plan q1 (filter puis select) :")
    q1.explain()
    println("Plan q2 (select puis filter) :")
    q2.explain()

    // ── Column Pruning : ne lire que les colonnes nécessaires
    println("\n── Column Pruning ──")
    df.select("name", "salary").filter(col("salary") > 5000).explain("formatted")
    // Le plan ne lira que name + salary depuis le CSV, pas les autres colonnes

    // ── Constant Folding
    println("\n── Constant Folding ──")
    df.filter(col("salary") > lit(100) * lit(50)).explain()
    // 100 * 50 est évalué à la planification → filter(salary > 5000)

    // ── Plan étendu (les 4 phases)
    println("\n── explain('extended') — Les 4 plans ──")
    df.filter(col("country") === "France").select("name", "salary").explain("extended")

    // ── Agrégation
    println("\n── Plan d'une agrégation groupBy ──")
    df.groupBy("country").agg(avg("salary")).explain("formatted")

    spark.stop()
  }
}
