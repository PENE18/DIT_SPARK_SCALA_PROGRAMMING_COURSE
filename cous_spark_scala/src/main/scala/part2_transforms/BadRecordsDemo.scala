package part2_transforms

/**
 * =============================================================
 *   08 — Gestion des enregistrements malformés : PERMISSIVE,
 *        DROPMALFORMED, FAILFAST, bad JSON
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BadRecordsDemo {
  def main(args: Array[String]): Unit = {

    val baseDir  = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvErrors = s"$baseDir/data/csv/employees_with_errors.csv"
    val jsonBad   = s"$baseDir/data/json/employees_bad.json"

    val spark = SparkSession.builder()
      .appName("BadRecords_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ══════════════════════════════════════════════
    // CSV — Mode PERMISSIVE
    // ══════════════════════════════════════════════
    println("\n── CSV PERMISSIVE ──")

    val schemaCorrupt = StructType(Seq(
      StructField("id",              IntegerType, nullable = true),
      StructField("name",            StringType,  nullable = true),
      StructField("age",             IntegerType, nullable = true),
      StructField("salary",          DoubleType,  nullable = true),
      StructField("country",         StringType,  nullable = true),
      StructField("_corrupt_record", StringType,  nullable = true)
    ))

    val dfPerm = spark.read
      .schema(schemaCorrupt)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .csv(csvErrors)

    println("Toutes les lignes :")
    dfPerm.show(truncate = false)

    println("Lignes OK :")
    dfPerm.filter(col("_corrupt_record").isNull).drop("_corrupt_record").show()

    println("Lignes corrompues :")
    dfPerm.filter(col("_corrupt_record").isNotNull).show(truncate = false)

    // ══════════════════════════════════════════════
    // CSV — Mode DROPMALFORMED
    // ══════════════════════════════════════════════
    println("\n── CSV DROPMALFORMED ──")
    val dfDrop = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv(csvErrors)

    dfDrop.show()
    println(s"Lignes conservées après DROPMALFORMED : ${dfDrop.count()}")

    // ══════════════════════════════════════════════
    // CSV — Mode FAILFAST
    // ══════════════════════════════════════════════
    println("\n── CSV FAILFAST ──")
    try {
      val dfFail = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(csvErrors)
      dfFail.show()
    } catch {
      case e: Exception =>
        println(s"✅ FAILFAST a levé une exception (attendu) : ${e.getClass.getSimpleName}")
    }

    // ══════════════════════════════════════════════
    // JSON — Mode PERMISSIVE (avec _corrupt_record)
    // ══════════════════════════════════════════════
    println("\n── JSON PERMISSIVE ──")
    val jsonSchema = StructType(Seq(
      StructField("id",              IntegerType, nullable = true),
      StructField("name",            StringType,  nullable = true),
      StructField("salary",          DoubleType,  nullable = true),
      StructField("_corrupt_record", StringType,  nullable = true)
    ))

    val dfJson = spark.read
      .schema(jsonSchema)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .json(jsonBad)

    dfJson.show(truncate = false)
    println("Lignes JSON corrompues :")
    dfJson.filter(col("_corrupt_record").isNotNull).show(truncate = false)

    // ══════════════════════════════════════════════
    // Options utiles : nullValue, emptyValue
    // ══════════════════════════════════════════════
    println("\n── Options nullValue / emptyValue ──")
    val dfNullOpts = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("emptyValue", "")
      .option("mode", "DROPMALFORMED")
      .csv(csvErrors)

    dfNullOpts.show()

    spark.stop()
  }
}
