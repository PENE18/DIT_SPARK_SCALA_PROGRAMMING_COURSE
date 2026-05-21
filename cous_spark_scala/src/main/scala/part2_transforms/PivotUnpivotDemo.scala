package part2_transforms

/**
 * =============================================================
 *   06 — Pivot & Unpivot : lignes ↔ colonnes
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotUnpivotDemo {
  def main(args: Array[String]): Unit = {

    val baseDir   = sys.env.getOrElse("PROJECT_ROOT", ".")
    val salesPath = s"$baseDir/data/csv/sales.csv"

    val spark = SparkSession.builder()
      .appName("Pivot_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(salesPath)
    println("Données source :"); df.show(5)

    // ── PIVOT basique
    println("── Pivot : total ventes par région et catégorie ──")
    val dfPivot = df.groupBy("region")
      .pivot("category", Seq("Electronics", "Furniture", "Accessories"))
      .agg(sum("amount").alias("total"))
    dfPivot.show()

    // ── PIVOT avec valeurs explicites (plus rapide)
    println("── Pivot avec valeurs explicites ──")
    val dfPivot2 = df.groupBy("region")
      .pivot("category", Seq("Electronics", "Furniture", "Accessories"))
      .agg(sum("amount"))
    dfPivot2.show()

    // ── UNPIVOT avec stack() (pre-Spark 3.4)
    println("── Unpivot avec stack() ──")
    val dfWide = Seq(
      ("North", Some(2650.0), Some(470.0),  Some(205.0)),
      ("South", Some(2590.0), None,          Some(85.0)),
      ("East",  Some(800.0),  Some(150.0),  Some(40.0)),
      ("West",  Some(820.0),  Some(160.0),  Some(330.0))
    ).toDF("region", "Electronics", "Furniture", "Accessories")

    dfWide.show()

    val dfUnpivot = dfWide.select(
      col("region"),
      expr("stack(3, 'Electronics', Electronics, 'Furniture', Furniture, 'Accessories', Accessories) as (category, total)")
    ).filter(col("total").isNotNull)

    println("Après unpivot :")
    dfUnpivot.show()

    spark.stop()
  }
}
