package part4_rdd

/**
 * =============================================================
 *   Part 4 — 01 : Introduction aux RDDs
 *   Création, inspection, lineage
 * =============================================================
 */

import org.apache.spark.sql.SparkSession

object RddIntroDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("RDD_Intro_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    // ══════════════════════════════════════════════
    // 1. Créer un RDD depuis une collection
    // ══════════════════════════════════════════════
    println("\n── 1. parallelize() ──")

    val rddNums = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    println(s"Éléments : ${rddNums.collect().mkString(", ")}")
    println(s"Partitions : ${rddNums.getNumPartitions}")

    val rdd4parts = sc.parallelize(Seq(1, 2, 3, 4, 5), numSlices = 4)
    println(s"Partitions forcées à 4 : ${rdd4parts.getNumPartitions}")

    // ══════════════════════════════════════════════
    // 2. Créer un RDD depuis un fichier texte
    // ══════════════════════════════════════════════
    println("\n── 2. textFile() ──")

    val rddLines = sc.textFile(csvPath)
    println(s"Nombre de lignes (avec header) : ${rddLines.count()}")
    println(s"Partitions : ${rddLines.getNumPartitions}")
    println("Premières lignes :")
    rddLines.take(3).foreach(println)

    // ══════════════════════════════════════════════
    // 3. wholeTextFiles — (nom_fichier, contenu)
    // ══════════════════════════════════════════════
    println("\n── 3. wholeTextFiles() ──")

    val rddFiles = sc.wholeTextFiles(s"$baseDir/data/csv/")
    rddFiles.collect().foreach { case (path, content) =>
      println(s"Fichier : $path")
      println(s"  Premières lignes : ${content.split("\n").take(2).mkString(" | ")}")
    }

    // ══════════════════════════════════════════════
    // 4. RDD depuis un DataFrame
    // ══════════════════════════════════════════════
    println("\n── 4. df.rdd ──")

    val df     = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)
    val rddRow = df.rdd

    println(s"RDD[Row] — nombre d'éléments : ${rddRow.count()}")
    println("Première Row :")
    val firstRow = rddRow.first()
    println(s"  name=${firstRow.getAs[String]("name")}, salary=${firstRow.getAs[Double]("salary")}")

    // ══════════════════════════════════════════════
    // 5. Inspecter le lineage (DAG)
    // ══════════════════════════════════════════════
    println("\n── 5. Lineage (toDebugString) ──")

    val rddFiltered = rddLines
      .filter(!_.startsWith("id"))          // exclure le header
      .map(_.split(","))
      .filter(_.length >= 5)
      .map(cols => (cols(4), cols(3).toDouble))  // (country, salary)

    println(rddFiltered.toDebugString)
    println(s"Partitioner : ${rddFiltered.partitioner}")

    // ══════════════════════════════════════════════
    // 6. Vérification du nombre de partitions
    // ══════════════════════════════════════════════
    println("\n── 6. Partitions ──")

    val rddRep   = rddNums.repartition(4)
    val rddCoal  = rddRep.coalesce(2)
    val rddGlom  = rddCoal.glom()

    println(s"Après repartition(4) : ${rddRep.getNumPartitions} partitions")
    println(s"Après coalesce(2)    : ${rddCoal.getNumPartitions} partitions")
    println("Contenu de chaque partition :")
    rddGlom.collect().zipWithIndex.foreach { case (arr, idx) =>
      println(s"  Partition $idx : ${arr.mkString(", ")}")
    }

    spark.stop()
    println("\nDémonstration Introduction RDD terminée.")
  }
}
