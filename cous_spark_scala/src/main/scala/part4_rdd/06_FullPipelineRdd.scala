

package part4_rdd

/**
 * =============================================================
 *   Part 4 — 06 : Pipeline complet RDD
 *   Word Count, analyse de logs, RDD ↔ DataFrame
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.HashPartitioner

object FullPipelineRdd {

  // ===========================================================
  // Case classes DOIVENT être déclarées hors du main
  // ===========================================================

  case class LogEntry(
                       ts: String,
                       level: String,
                       message: String,
                       ip: String
                     )

  case class Employee(
                       name: String,
                       age: Int,
                       salary: Double,
                       country: String,
                       dept: String
                     )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Part4_FullPipeline_RDD")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    import spark.implicits._

    println("\n" + "=" * 60)
    println("  PART 4 — Pipeline complet RDD")
    println("=" * 60)

    // ══════════════════════════════════════════════════════════
    // SECTION 1 — Word Count classique
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  1. Word Count")
    println("─" * 50)

    val corpus = sc.parallelize(Seq(
      "Spark est un moteur de traitement de données distribué",
      "Les RDDs sont la brique fondamentale de Spark",
      "Spark SQL et les DataFrames simplifient le traitement",
      "Le partitionnement est clé pour les performances Spark",
      "Les accumulateurs et broadcast variables sont utiles dans Spark",
      "Spark est rapide grâce à son traitement en mémoire"
    ))

    val wordCount = corpus
      .flatMap(_.toLowerCase.split("\\s+"))
      .filter(w => w.nonEmpty && w.length > 2)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("Top 15 mots :")

    wordCount.take(15).foreach {
      case (word, count) =>
        println(f"  ${word}%-20s : $count")
    }

    println(s"\nVocabulaire unique : ${wordCount.count()} mots")
    println(s"Total occurrences  : ${wordCount.values.sum().toLong}")

    // ══════════════════════════════════════════════════════════
    // SECTION 2 — Analyse de logs simulés
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  2. Analyse de logs")
    println("─" * 50)

    val logData = sc.parallelize(Seq(
      "2024-01-15 08:00:01 INFO  User login successful from 192.168.1.10",
      "2024-01-15 08:00:15 ERROR Database connection failed from 10.0.0.5",
      "2024-01-15 08:01:00 WARN  High memory usage detected from 192.168.1.10",
      "2024-01-15 08:01:30 ERROR Timeout processing request from 10.0.0.3",
      "2024-01-15 08:02:00 INFO  Batch job completed from 192.168.1.20",
      "2024-01-15 08:02:45 ERROR Authentication failed from 10.0.0.5",
      "2024-01-15 08:03:00 INFO  Cache cleared from 192.168.1.10",
      "2024-01-15 08:03:30 ERROR Disk space critical from 10.0.0.7",
      "2024-01-15 08:04:00 WARN  Slow query detected from 192.168.1.30",
      "2024-01-15 08:04:15 INFO  User logout from 192.168.1.10",
      "2024-01-15 CORRUPT_LINE_MISSING_FIELDS",
      "2024-01-15 08:05:00 ERROR Connection refused from 10.0.0.5"
    ))

    // Accumulateurs
    val totalLinesAcc = sc.longAccumulator("total_lines")
    val parsedLinesAcc = sc.longAccumulator("parsed_lines")
    val errorLinesAcc = sc.longAccumulator("error_lines")

    val parsedLogs = logData.flatMap { line =>

      totalLinesAcc.add(1L)

      val parts = line.split(" ", 5)

      if (parts.length >= 5) {

        parsedLinesAcc.add(1L)

        if (parts(2) == "ERROR") {
          errorLinesAcc.add(1L)
        }

        Some(
          LogEntry(
            s"${parts(0)} ${parts(1)}",
            parts(2),
            parts(3),
            parts(4).split(" ").last
          )
        )

      } else {
        None
      }

    }.cache()

    println("=== Métriques parsing ===")

    parsedLogs.count()

    println(s"  Total lignes   : ${totalLinesAcc.value}")
    println(s"  Lignes parsées : ${parsedLinesAcc.value}")
    println(s"  Lignes erreur  : ${errorLinesAcc.value}")

    println(
      f"  Taux parsing   : ${
        parsedLinesAcc.value.toDouble / totalLinesAcc.value * 100
      }%.1f%%"
    )

    println("\n=== Logs par niveau ===")

    parsedLogs
      .map(e => (e.level, 1L))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach {
        case (level, cnt) =>
          println(f"  ${level}%-8s : $cnt")
      }

    println("\n=== IPs suspectes (>= 2 erreurs) ===")

    parsedLogs
      .filter(_.level == "ERROR")
      .map(e => (e.ip, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= 2)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach {
        case (ip, cnt) =>
          println(s"  $ip : $cnt erreurs")
      }

    parsedLogs.unpersist()

    // ══════════════════════════════════════════════════════════
    // SECTION 3 — RDD vers DataFrame
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  3. RDD → DataFrame")
    println("─" * 50)

    val rawPipeData = sc.parallelize(Seq(
      "Alice|32|5200.0|France|Engineering",
      "Bob|45|4800.0|UK|Sales",
      "Carol|28|6100.0|France|Engineering",
      "Dave|55|7000.0|Germany|HR",
      "Eve|38|5500.0|UK|Sales",
      "Frank|42|6500.0|Germany|Engineering",
      "BAD_LINE_MISSING_FIELDS",
      "Grace|33|5800.0|France|HR"
    ))

    // ================================
    // Méthode 1 — case class + toDF()
    // ================================

    val employeeRdd = rawPipeData
      .filter(_.count(_ == '|') == 4)
      .map { line =>

        val p = line.split("\\|")

        Employee(
          p(0),
          p(1).toInt,
          p(2).toDouble,
          p(3),
          p(4)
        )
      }

    val df1 = employeeRdd.toDF()

    println("Méthode 1 — case class.toDF() :")

    df1.printSchema()

    df1.show()

    // ================================
    // Méthode 2 — Row + Schema
    // ================================

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("dept", StringType, nullable = true)
    ))

    val rowRdd = rawPipeData
      .filter(_.count(_ == '|') == 4)
      .map { line =>

        val p = line.split("\\|")

        Row(
          p(0),
          p(1).toInt,
          p(2).toDouble,
          p(3),
          p(4)
        )
      }

    val df2 = spark.createDataFrame(rowRdd, schema)

    println("Méthode 2 — Row + schéma explicite :")

    df2.show()

    // ══════════════════════════════════════════════════════════
    // SECTION 4 — DataFrame vers RDD
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  4. DataFrame → RDD")
    println("─" * 50)

    val rowRdd2 = df1.rdd

    println("RDD[Row] — accès par nom :")

    rowRdd2.take(3).foreach { row =>
      println(
        s"  ${row.getAs[String]("name")} : ${row.getAs[Double]("salary")}"
      )
    }

    val typedRdd = df1.as[Employee].rdd

    println("\nRDD[Employee] — accès typé :")

    typedRdd.take(3).foreach { e =>
      println(s"  ${e.name} (${e.dept}) : ${e.salary}")
    }

    // ══════════════════════════════════════════════════════════
    // SECTION 5 — Pipeline mixte RDD + DataFrame
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  5. Pipeline mixte RDD + DataFrame")
    println("─" * 50)

    val rddClean = rawPipeData
      .filter(_.count(_ == '|') == 4)
      .map { line =>

        val p = line.split("\\|")

        (
          p(0),
          p(1).toInt,
          p(2).toDouble,
          p(3),
          p(4)
        )
      }
      .toDF("name", "age", "salary", "country", "dept")

    println("Salaire moyen par département :")

    rddClean
      .groupBy("dept")
      .agg(
        avg("salary").alias("avg_salary"),
        count("*").alias("headcount")
      )
      .orderBy(desc("avg_salary"))
      .show()

    // ══════════════════════════════════════════════════════════
    // SECTION 6 — Partitionnement avancé
    // ══════════════════════════════════════════════════════════

    println("\n" + "─" * 50)
    println("  6. Partitionnement avancé")
    println("─" * 50)

    val salesRdd = sc.parallelize(Seq(
        ("France", 1500.0),
        ("UK", 2000.0),
        ("France", 1800.0),
        ("Germany", 3000.0),
        ("UK", 1200.0),
        ("France", 2200.0),
        ("Germany", 1900.0),
        ("UK", 2500.0)
      ))
      .partitionBy(new HashPartitioner(3))
      .persist()

    println(s"Partitions  : ${salesRdd.getNumPartitions}")
    println(s"Partitioner : ${salesRdd.partitioner}")

    println("\nTotal ventes par pays :")

    salesRdd
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
      .foreach {
        case (country, total) =>
          println(f"  $country%-10s : $total%.0f")
      }

    println("\nMoyenne ventes par pays :")

    salesRdd
      .aggregateByKey((0.0, 0))(
        seqOp = {
          case ((s, c), v) =>
            (s + v, c + 1)
        },
        combOp = {
          case ((s1, c1), (s2, c2)) =>
            (s1 + s2, c1 + c2)
        }
      )
      .mapValues {
        case (sum, cnt) =>
          sum / cnt
      }
      .sortByKey()
      .collect()
      .foreach {
        case (country, avgSale) =>
          println(f"  $country%-10s : $avgSale%.2f")
      }

    salesRdd.unpersist()

    // ══════════════════════════════════════════════════════════
    // FIN
    // ══════════════════════════════════════════════════════════

    spark.stop()

    println("\n SparkSession arrêtée. Pipeline Part 4 RDD terminé !\n")
  }
}
