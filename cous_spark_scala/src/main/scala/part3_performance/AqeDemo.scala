package part3_performance

/**
 * =============================================================
 *   05 — Adaptive Query Execution (AQE)
 *        - Coalescence dynamique des partitions
 *        - Changement dynamique de stratégie de jointure
 *        - Optimisation du skew join
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AqeDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    // ── Spark avec AQE activé (défaut en 3.2+)
    val spark = SparkSession.builder()
      .appName("AQE_Demo")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled",                         "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled",      "true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",    "64mb")
      .config("spark.sql.adaptive.skewJoin.enabled",                "true")
      .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",  "5")
      .config("spark.sql.shuffle.partitions",                       "50")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── Afficher la configuration AQE
    println("\n── Configuration AQE ──")
    val configs = Seq(
      "spark.sql.adaptive.enabled",
      "spark.sql.adaptive.coalescePartitions.enabled",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      "spark.sql.adaptive.skewJoin.enabled",
      "spark.sql.shuffle.partitions",
      "spark.sql.autoBroadcastJoinThreshold"
    )
    configs.foreach { c =>
      try   { println(s"  $c = ${spark.conf.get(c)}") }
      catch { case _: Exception => println(s"  $c = (non défini)") }
    }

    // ── Feature 1 : Coalescence dynamique des partitions de shuffle
    println("\n── Feature 1 : Coalescence dynamique des partitions ──")
    println(s"Partitions avant shuffle : ${df.rdd.getNumPartitions}")

    val dfAgg = df.groupBy("country", "department").agg(avg("salary").alias("avg_salary"))
    dfAgg.show()

    // ── Feature 2 : Changement dynamique de stratégie de jointure
    println("\n── Feature 2 : Changement dynamique de jointure ──")
    val smallDept = Seq(
      ("Engineering", "Tech"), ("Sales", "Commercial"), ("HR", "Support")
    ).toDF("department", "category")

    val result = df.join(smallDept, "department", "left")
    println("Plan avec AQE (peut montrer BroadcastHashJoin dynamique) :")
    result.explain("formatted")
    result.show(5)

    // ── Feature 3 : Skew join — simulation de données skewées
    println("\n── Feature 3 : Simulation skew join ──")
    val skewedData = (
      (1 to 99).map(i => (i, "France",  5000.0)) ++
      (1 to 4).map(i  => (i + 100, "UK",      4000.0)) ++
      (1 to 2).map(i  => (i + 110, "Germany", 6000.0))
    )
    val dfSkewed = skewedData.toDF("id", "country", "salary")

    val countryRef = Seq(
      ("France", "FR"), ("UK", "GB"), ("Germany", "DE")
    ).toDF("country", "code")

    val resultSkewed = dfSkewed.join(countryRef, "country")
    resultSkewed.groupBy("country", "code").count().show()

    // ── Désactiver AQE pour comparer
    println("\n── Désactiver AQE ──")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df.groupBy("country").agg(avg("salary")).explain()
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    println(
      """
        |Résumé AQE :
        |  Feature 1 : Fusionne les petites partitions de shuffle → moins de tâches
        |  Feature 2 : Peut passer de SortMergeJoin à BroadcastHashJoin à l'exécution
        |  Feature 3 : Divise les partitions skewées → évite les stragglers
        |  Activé par défaut depuis Spark 3.2
        |""".stripMargin)

    spark.stop()
  }
}
