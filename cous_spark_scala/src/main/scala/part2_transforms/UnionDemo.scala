package part2_transforms

/**
 * =============================================================
 *   05 — Union & UnionByName : correspondance par position vs nom
 * =============================================================
 */

import org.apache.spark.sql.SparkSession

object UnionDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Union_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df1 = Seq((1, "Alice", 3000.0), (2, "Bob",   4000.0)).toDF("id", "name", "salary")
    val df2 = Seq((3, "Carol", 5000.0), (1, "Alice", 3000.0)).toDF("id", "name", "salary")

    println("df1 :"); df1.show()
    println("df2 :"); df2.show()

    // ── union() — GARDE les doublons (comme UNION ALL)
    println("\n── union() — garde les doublons ──")
    df1.union(df2).show()

    // ── union().distinct() — supprime les doublons
    println("\n── union().distinct() ──")
    df1.union(df2).distinct().show()

    // ── Piège : correspondance par position
    println("\n── Piège position — colonnes dans ordre différent ──")
    val df3 = Seq(("Dave", 4, 4500.0)).toDF("name", "id", "salary")  // ordre différent
    println("df3 (colonnes inversées : name, id, salary) :"); df3.show()
    println("df1.union(df3) — FAUX (par position) :")
    df1.union(df3).show()  // "Dave" ira dans la colonne id !

    // ── unionByName() — par nom de colonne (correct)
    println("\n── unionByName() — par NOM de colonne ──")
    df1.unionByName(df3).show()

    // ── allowMissingColumns
    println("\n── unionByName(allowMissingColumns=true) ──")
    val df4 = Seq((5, "Eve")).toDF("id", "name")  // pas de colonne salary
    df1.unionByName(df4, allowMissingColumns = true).show()

    // ── Union de plusieurs DataFrames
    println("\n── Union de plusieurs DataFrames ──")
    val dfA = Seq((10, "X")).toDF("id", "name")
    val dfB = Seq((11, "Y")).toDF("id", "name")
    val dfC = Seq((12, "Z")).toDF("id", "name")

    // Sélectionner colonnes communes avant union multiple
    val dfs = Seq(df1.select("id", "name"), dfA, dfB, dfC)
    val dfAll = dfs.reduce(_ union _)
    dfAll.show()

    spark.stop()
  }
}
