package part2_transforms

/**
 * =============================================================
 *   01 — Colonnes : withColumn, withColumnRenamed, drop, cast
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Columns_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)
    println("DataFrame source :"); df.show(3)

    // ── withColumn : ajouter
    println("\n── withColumn ──")
    val df2 = df
      .withColumn("annual_salary", col("salary") * 12)
      .withColumn("is_senior",     col("age") >= 55)
      .withColumn("name_upper",    upper(col("name")))
      .withColumn("source",        lit("csv_import"))
      .withColumn("salary",        round(col("salary"), 0))  // remplace l'existante

    df2.select("name_upper", "salary", "annual_salary", "is_senior", "source").show(5)

    // ── withColumns (Spark 3.3+) — en Scala, on enchaîne les withColumn
    println("\n── withColumns équivalent (enchaînement) ──")
    val df3 = df
      .withColumn("tax",    col("salary") * 0.2)
      .withColumn("net",    col("salary") * 0.8)
      .withColumn("source", lit("batch"))
    df3.select("name", "salary", "tax", "net", "source").show(5)

    // ── withColumnRenamed
    println("\n── withColumnRenamed ──")
    val df4 = df
      .withColumnRenamed("name",    "full_name")
      .withColumnRenamed("country", "nation")
    df4.show(3)

    // ── select + alias
    println("\n── select + alias ──")
    df.select(
      col("id"),
      col("name").alias("full_name"),
      col("salary").alias("monthly_salary"),
      col("country").alias("nation")
    ).show(3)

    // ── drop
    println("\n── drop ──")
    df.drop("hire_date", "department").show(3)

    // ── cast
    println("\n── cast ──")
    df.withColumn("age",    col("age").cast("string"))
      .withColumn("salary", col("salary").cast("integer"))
      .printSchema()

    spark.stop()
  }
}
