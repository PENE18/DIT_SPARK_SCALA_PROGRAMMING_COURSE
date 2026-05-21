package part2_transforms

/**
 * =============================================================
 *   03 — Explode : explode, explode_outer, posexplode, map
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ExplodeDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Explode_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ── Données : tableau de compétences
    val dataArray = Seq(
      (1, "Alice", Seq("Python", "Spark", "SQL")),
      (2, "Bob",   Seq("Java", "Scala")),
      (3, "Carol", Seq.empty[String]),
      (4, "Dave",  null.asInstanceOf[Seq[String]])
    ).toDF("id", "name", "skills")

    println("DataFrame source :"); dataArray.show(truncate = false)

    // ── explode() — supprime vide/null
    println("\n── explode() ──")
    dataArray.withColumn("skill", explode(col("skills"))).show()

    // ── explode_outer() — garde les lignes vides
    println("\n── explode_outer() ──")
    dataArray.withColumn("skill", explode_outer(col("skills"))).show()

    // ── posexplode() — avec index de position
    println("\n── posexplode() ──")
    dataArray.select(col("id"), col("name"), posexplode(col("skills")).as(Seq("pos", "skill"))).show()

    // ── Données : colonne Map
    val schemaMap = StructType(Seq(
      StructField("id",     IntegerType),
      StructField("name",   StringType),
      StructField("scores", MapType(StringType, IntegerType))
    ))

    val dataMap = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        org.apache.spark.sql.Row(1, "Alice", Map("Python" -> 5, "Spark" -> 4)),
        org.apache.spark.sql.Row(2, "Bob",   Map("Java"   -> 3, "Scala" -> 5))
      )),
      schemaMap
    )

    println("\n── Explode sur une colonne Map ──")
    dataMap.select(
      col("id"), col("name"),
      explode(col("scores")).as(Seq("competence", "score"))
    ).show()

    spark.stop()
  }
}
