package part3_performance

/**
 * =============================================================
 *   06 — Gestion des Nulls
 * =============================================================
 */

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NullsDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Nulls_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Seq(
      StructField("id",      IntegerType),
      StructField("name",    StringType),
      StructField("age",     IntegerType),
      StructField("salary",  DoubleType),
      StructField("country", StringType)
    ))

    val rows = Seq(
      Row(1, "Alice",  32,   5200.0,    "France"),
      Row(2, "Bob",    null, 4800.0,    "UK"),
      Row(3, null,     28,   null,       "France"),
      Row(4, "Dave",   55,   7100.0,    null),
      Row(5, "Eve",    38,   Double.NaN, "France"),
      Row(6, "Frank",  null, null,       null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.show()

    // ── Compter les nulls par colonne
    println("── Nulls par colonne ──")
    df.select(df.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*).show()

    // ── isNull vs isnan
    println("── isNull (SQL null) ──")
    df.filter(col("salary").isNull).show()

    println("── isnan (NaN flottant) ──")
    df.filter(isnan(col("salary"))).show()

    println("── Les deux combinés ──")
    df.filter(col("salary").isNull || isnan(col("salary"))).show()

    // ── fillna
    println("── fillna ──")
    df.na.fill(Map[String, Any]("name" -> "Inconnu", "salary" -> 0.0, "country" -> "N/A", "age" -> -1)).show()

    // ── dropna
    println("── dropna() — une null quelconque ──")
    df.na.drop().show()

    println("── dropna(how='all') — toutes nulles ──")
    df.na.drop("all").show()

    println("── dropna(subset=['name']) ──")
    df.na.drop(Seq("name")).show()

    // ── na.replace
    println("── na.replace ──")
    df.na.replace("country", Map("France" -> "FR", "UK" -> "GB")).show()

    // ── coalesce
    println("── coalesce ──")
    df.withColumn("country_safe", coalesce(col("country"), lit("Inconnu"))).show()

    // ── eqNullSafe
    println("── eqNullSafe (null == null → True) ──")
    df.filter(col("country").eqNullSafe(null)).show()

    // ── orderBy asc_nulls_last
    println("── orderBy asc_nulls_last ──")
    df.orderBy(col("salary").asc_nulls_last).show()

    // ── concat_ws ignore les nulls
    println("── concat_ws (ignore les nulls) ──")
    df.withColumn("info", concat_ws(" | ", col("name"), col("country"))).show()

    // ── Pipeline complet de nettoyage
    println("── Pipeline de nettoyage complet ──")
    val dfClean = df
      .na.drop("all")
      .na.fill(Map[String, Any]("name" -> "Inconnu", "country" -> "N/A"))
      .withColumn("salary",
        when(col("salary").isNull || isnan(col("salary")), lit(0.0))
        .otherwise(col("salary")))
      .na.drop(Seq("id"))

    dfClean.show()

    spark.stop()
  }
}
