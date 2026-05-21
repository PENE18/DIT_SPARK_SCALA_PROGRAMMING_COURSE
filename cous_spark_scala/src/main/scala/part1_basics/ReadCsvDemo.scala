package part1_basics

/**
 * =============================================================
 *   02 — Lire des CSV : schéma, options, modes d'erreur
 * =============================================================
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadCsvDemo {
  def main(args: Array[String]): Unit = {


    val baseDir  = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvClean  = s"$baseDir/data/csv/employees.csv"
    val csvErrors = s"$baseDir/data/csv/employees_with_errors.csv"
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    val spark = SparkSession.builder()
      .appName("ReadCSV_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ── 1. Lecture simple avec inferSchema
    println("\n── 1. InferSchema (déconseillé en prod) ──")
    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(csvClean)
    df1.printSchema()
    df1.show(3)

    // ── 2. Schéma explicite (recommandé)
    println("\n── 2. Schéma explicite ──")
    val schema = StructType(Seq(
      StructField("id",         IntegerType, nullable = false),
      StructField("name",       StringType,  nullable = true),
      StructField("age",        IntegerType, nullable = true),
      StructField("salary",     DoubleType,  nullable = true),
      StructField("country",    StringType,  nullable = true),
      StructField("department", StringType,  nullable = true),
      StructField("hire_date",  StringType,  nullable = true)
    ))
    val df2 = spark.read.schema(schema).option("header", "true").csv(csvClean)
    df2.printSchema()
    df2.show(3)

    // ── 3. Statistiques
    println("\n── 3. Inspection ──")
    println("Colonnes : " + df2.columns.mkString(", "))
    println(s"Nombre de lignes : ${df2.count()}")
    df2.describe("age", "salary").show()

    // ── 4. Mode DROPMALFORMED
    println("\n── 4. DROPMALFORMED ──")
    val dfDrop = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .csv(csvErrors)
    println(s"Lignes après DROPMALFORMED : ${dfDrop.count()}")
    dfDrop.show()

    // ── 5. Mode PERMISSIVE avec _corrupt_record
    println("\n── 5. PERMISSIVE + _corrupt_record ──")
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
    dfPerm.show(truncate = false)
    println("Lignes corrompues :")
    dfPerm.filter(dfPerm("_corrupt_record").isNotNull).show(truncate = false)

    spark.stop()
    println("\nDémonstration lecture CSV terminée.")
  }
}
