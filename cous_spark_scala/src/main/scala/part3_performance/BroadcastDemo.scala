package part3_performance

/**
 * =============================================================
 *   04 — Broadcast Join & Variables Broadcast
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Broadcast_Demo")
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", "10mb")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val dfEmployees = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── Petite table de dimension
    val dfDept = Seq(
      ("Engineering", 1, "Tech",       500000.0),
      ("Sales",        2, "Commercial", 300000.0),
      ("HR",           3, "Support",    150000.0)
    ).toDF("department", "dept_id", "category", "budget")

    // ── Broadcast join automatique (si table < seuil)
    println("\n── Broadcast join (automatique) ──")
    val resultAuto = dfEmployees.join(dfDept, "department", "left")
    resultAuto.select("name", "department", "category", "budget").show()
    println("Plan (chercher BroadcastHashJoin) :")
    resultAuto.explain()

    // ── Broadcast join manuel (forcer)
    println("\n── Broadcast join manuel avec broadcast() hint ──")
    val resultManual = dfEmployees.join(broadcast(dfDept), "department", "left")
    resultManual.select("name", "department", "category").show()

    // ── Seuil broadcast
    println(s"\nSeuil autoBroadcast : ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")

    // ── Variable broadcast pour UDF (lookup dictionary)
    println("\n── Variable broadcast (dictionnaire lookup) ──")
    val countryLookup = Map(
      "France"  -> "République Française",
      "UK"      -> "Royaume-Uni",
      "USA"     -> "États-Unis d'Amérique",
      "Germany" -> "République Fédérale d'Allemagne"
    )

    // Broadcaster le dictionnaire sur tous les executors
    val bcLookup = spark.sparkContext.broadcast(countryLookup)

    val expandCountryUdf = udf((code: String) =>
      bcLookup.value.getOrElse(code, s"Inconnu ($code)")
    )

    dfEmployees
      .withColumn("pays_complet", expandCountryUdf(col("country")))
      .select("name", "country", "pays_complet")
      .show()

    // Libérer la variable broadcast
    bcLookup.unpersist()
    println("Variable broadcast libérée.")

    println(
      """
        |Stratégies de jointure :
        |  BroadcastHashJoin  → petite table < seuil, pas de shuffle (le plus rapide)
        |  ShuffleHashJoin    → tables moyennes, shuffle + hash map
        |  SortMergeJoin      → grandes tables (défaut), shuffle + tri des 2 côtés
        |  CartesianJoin      → cross join, N×M lignes (dangereux !)
        |""".stripMargin)

    spark.stop()
  }
}
