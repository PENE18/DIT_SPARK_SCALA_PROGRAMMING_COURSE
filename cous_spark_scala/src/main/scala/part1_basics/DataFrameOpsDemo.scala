package part1_basics

/**
 * =============================================================
 *   03 — Opérations DataFrame : select, withColumn, groupBy, join
 * =============================================================
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameOpsDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath  = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("DataFrame_Ops")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── select
    println("\n── select ──")
    df.select("name", "salary", "country").show(5)
    df.select(col("name"), col("salary").alias("monthly_salary")).show(5)

    // ── withColumn
    println("\n── withColumn ──")
    val df2 = df
      .withColumn("annual_salary", col("salary") * 12)
      .withColumn("is_senior",     col("age") >= 50)
      .withColumn("name_upper",    upper(col("name")))
      .withColumn("source",        lit("employees_csv"))
      .withColumn("salary",        round(col("salary"), 0).cast("integer"))

    df2.select("name_upper", "salary", "annual_salary", "is_senior").show(5)

    // ── withColumnRenamed & drop
    println("\n── rename & drop ──")
    val df3 = df.withColumnRenamed("country", "nation").drop("hire_date")
    df3.show(3)

    // ── Agrégations
    println("\n── groupBy + agg ──")
    df.groupBy("country").agg(
      count("id").alias("nb"),
      avg("salary").alias("avg_salary"),
      max("age").alias("max_age")
    ).orderBy(desc("avg_salary")).show()

    // ── Tri
    println("\n── orderBy ──")
    df.select("name", "salary").orderBy(desc("salary")).show(5)

    // ── distinct / limit
    println("\n── distinct countries ──")
    df.select("country").distinct().orderBy("country").show()

    println("\n── limit(3) ──")
    df.limit(3).show()

    spark.stop()
  }
}
