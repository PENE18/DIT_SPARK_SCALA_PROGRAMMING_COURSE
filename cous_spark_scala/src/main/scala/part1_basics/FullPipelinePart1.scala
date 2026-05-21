package part1_basics

/**
 * =============================================================
 *   Part 1 — Pipeline complet : SparkSession, CSV, Filtres
 * =============================================================
 * Fichier de données : data/csv/employees.csv
 * Lancer depuis la racine du projet
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

object FullPipelinePart1 {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.sparkproject").setLevel(Level.ERROR)
    System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.properties")
    // ─────────────────────────────────────────────
    // 1. Créer la SparkSession
    // ─────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Part1_FullPipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println("\n SparkSession créée\n")

    // ─────────────────────────────────────────────
    // 2. Définir le schéma manuellement (bonne pratique)
    // ─────────────────────────────────────────────
    val schema = StructType(Seq(
      StructField("id",         IntegerType, nullable = false),
      StructField("name",       StringType,  nullable = true),
      StructField("age",        IntegerType, nullable = true),
      StructField("salary",     DoubleType,  nullable = true),
      StructField("country",    StringType,  nullable = true),
      StructField("department", StringType,  nullable = true),
      StructField("hire_date",  StringType,  nullable = true)
    ))

    // ─────────────────────────────────────────────
    // 3. Lire le CSV
    // ─────────────────────────────────────────────
    val df = spark.read
      .schema(schema)
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(csvPath)

    println(" Schéma :")
    df.printSchema()
    println(s" Nombre de lignes : ${df.count()}")
    println("\n Aperçu (5 lignes) :")
    df.show(5)

    // ─────────────────────────────────────────────
    // 4. Inspecter
    // ─────────────────────────────────────────────
    println(" Colonnes : " + df.columns.mkString(", "))
    println(" Types    : " + df.dtypes.map { case (n, t) => s"$n:$t" }.mkString(", "))
    println("\n Statistiques descriptives :")
    df.describe("age", "salary").show()

    // ─────────────────────────────────────────────
    // 5. Filtres
    // ─────────────────────────────────────────────
    println("\n─── Filtre : âge entre 30 et 50, salaire > 4000 ───")
    val dfFiltered = df
      .filter(col("age").between(30, 50))
      .filter(col("salary") > 4000)
      .filter(col("salary").isNotNull)
      .filter(col("country").isin("France", "UK", "USA"))
    dfFiltered.show()

    println("\n─── Filtre : pays France ou Germany, NOT département HR ───")
    val dfFiltered2 = df.filter(
      ((col("country") === "France") || (col("country") === "Germany")) &&
      !col("department").like("%HR%")
    )
    dfFiltered2.show()

    // ─────────────────────────────────────────────
    // 6. Transformations — select, withColumn, rename
    // ─────────────────────────────────────────────
    println("\n─── Transformations ───")
    val dfTransformed = dfFiltered
      .withColumn("name_upper",  upper(col("name")))
      .withColumn("tax",         col("salary") * lit(0.2))
      .withColumn("net_salary",  col("salary") * lit(0.8))
      .withColumnRenamed("country", "nation")

    dfTransformed.select("id", "name_upper", "salary", "tax", "net_salary", "nation").show()

    // ─────────────────────────────────────────────
    // 7. Agrégations avec groupBy
    // ─────────────────────────────────────────────
    println("\n─── Agrégation par pays ───")
    val dfAgg = df.groupBy("country")
      .agg(
        count("id").alias("nb_employes"),
        avg("salary").alias("salaire_moyen"),
        avg("age").alias("age_moyen")
      )
      .orderBy(desc("salaire_moyen"))

    dfAgg.show()

    println("\n─── Agrégation par département ───")
    df.groupBy("department")
      .agg(
        count("*").alias("effectif"),
        avg("salary").alias("salaire_moyen")
      )
      .orderBy("department")
      .show()

    // ─────────────────────────────────────────────
    // 8. Sélection et tri
    // ─────────────────────────────────────────────
    println("\n─── Top 5 salaires ───")
    df.select("name", "salary", "country")
      .orderBy(desc("salary"))
      .show(5)

    // ─────────────────────────────────────────────
    // 9. Vue SQL temporaire
    // ─────────────────────────────────────────────
    df.createOrReplaceTempView("employees")

    println("\n─── Requête SQL : ingénieurs avec salaire > 5000 ───")
    spark.sql(
      """
        SELECT name, age, salary, country
        FROM employees
        WHERE department = 'Engineering'
          AND salary > 5000
        ORDER BY salary DESC
      """
    ).show()

    // ─────────────────────────────────────────────
    // 10. Écrire les résultats (en local)
    // ─────────────────────────────────────────────
    val outputPath = s"$baseDir/output/part1_results"
    new File(s"$outputPath/agg_by_country").mkdirs()

    dfAgg.write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/agg_by_country")

    println(s"\n Résultats écrits dans : $outputPath")

    // ─────────────────────────────────────────────
    // Fin
    // ─────────────────────────────────────────────
    spark.stop()
    println("\n SparkSession arrêtée. Pipeline Part 1 terminé !\n")
  }
}
