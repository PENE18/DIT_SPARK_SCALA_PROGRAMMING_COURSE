package part2_transforms

/**
 * =============================================================
 *   07 — Lire & Aplatir du JSON : struct, array, from_json
 * =============================================================
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JsonFlattenDemo {
  def main(args: Array[String]): Unit = {

    val baseDir  = sys.env.getOrElse("PROJECT_ROOT", ".")
    val jsonPath = s"$baseDir/data/json/employees_nested.json"

    val spark = SparkSession.builder()
      .appName("JSON_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ── Lire le JSON imbriqué
    println("\n── Schéma JSON inféré ──")
    val df = spark.read.option("multiLine", "true").json(jsonPath)
    df.printSchema()

    println("\n── Aperçu brut ──")
    df.show(3, truncate = false)

    // ── Accéder aux champs imbriqués
    println("\n── Accès aux champs struct (dot notation) ──")
    df.select("id", "name", "address.city", "address.country",
              "scores.python", "scores.spark").show()

    // ── Aplatir le struct address + scores
    println("\n── Aplatissement du struct ──")
    val dfFlat = df.select(
      col("id"),
      col("name"),
      col("age"),
      col("salary"),
      col("address.city").alias("city"),
      col("address.country").alias("country"),
      col("address.zip").alias("zip"),
      col("scores.python").alias("python_score"),
      col("scores.spark").alias("spark_score"),
      col("scores.sql").alias("sql_score")
    )
    dfFlat.show()

    // ── Aplatir le tableau skills
    println("\n── Explode du tableau skills ──")
    df.select(col("id"), col("name"), explode_outer(col("skills")).alias("skill")).show()

    // ── Aplatir struct + tableau ensemble
    println("\n── Struct + Tableau ensemble ──")
    val dfCombined = df.select(
      col("id"),
      col("name"),
      col("address.city").alias("city"),
      col("scores.python").alias("python_score"),
      explode_outer(col("skills")).alias("skill")
    )
    dfCombined.show()

    // ── Fonction flatten récursive
    println("\n── Flatten récursif ──")
    def flattenDf(df: DataFrame): DataFrame = {
      var result = df
      var hasStruct = result.schema.fields.exists(_.dataType.isInstanceOf[StructType])
      while (hasStruct) {
        val structCols = result.schema.fields.filter(_.dataType.isInstanceOf[StructType])
        val colName    = structCols.head.name
        val nestedCols = result.select(s"$colName.*").columns
          .map(sub => col(s"$colName.$sub").alias(s"${colName}_$sub"))
        val otherCols  = result.columns.filter(_ != colName).map(col)
        result    = result.select(otherCols ++ nestedCols: _*)
        hasStruct = result.schema.fields.exists(_.dataType.isInstanceOf[StructType])
      }
      result
    }

    val dfFullyFlat = flattenDf(df.drop("skills"))
    println("Schéma après flatten récursif :")
    dfFullyFlat.printSchema()
    dfFullyFlat.show()

    // ── from_json : parser une colonne JSON string
    println("\n── from_json : colonne JSON string ──")
    val dfStr = Seq(
      (1, """{"city": "Paris", "country": "France"}"""),
      (2, """{"city": "London", "country": "UK"}""")
    ).toDF("id", "address_json")

    val jsonSchema = StructType(Seq(
      StructField("city",    StringType),
      StructField("country", StringType)
    ))
    val dfParsed = dfStr.withColumn("address_parsed", from_json(col("address_json"), jsonSchema))
    dfParsed
      .withColumn("city",    col("address_parsed.city"))
      .withColumn("country", col("address_parsed.country"))
      .drop("address_parsed")
      .show()

    spark.stop()
  }
}
