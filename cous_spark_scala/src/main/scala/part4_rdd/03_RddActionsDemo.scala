/*
package part4_rdd

/**
 * =============================================================
 *   Part 4 — 03 : Actions RDD
 *   collect, count, reduce, fold, aggregate, stats, save
 * =============================================================
 */

import org.apache.spark.sql.SparkSession

object RddActionsDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD_Actions_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq(3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0, 5.0, 3.0))
    val rddInt = sc.parallelize(1 to 10)

    // ══════════════════════════════════════════════
    // 1. Actions de collecte
    // ══════════════════════════════════════════════
    println("\n── 1. Actions de collecte ──")

    val all: Array[Double] = rdd.collect()
    println(s"collect()           : ${all.mkString(", ")}")

    val first3: Array[Double] = rdd.take(3)
    println(s"take(3)             : ${first3.mkString(", ")}")

    val first: Double = rdd.first()
    println(s"first()             : $first")

    val top3: Array[Double] = rdd.top(3)
    println(s"top(3)              : ${top3.mkString(", ")}")

    val smallest3: Array[Double] = rdd.takeOrdered(3)
    println(s"takeOrdered(3)      : ${smallest3.mkString(", ")}")

    val sample: Array[Double] = rdd.takeSample(withReplacement = false, num = 4, seed = 42)
    println(s"takeSample(false,4) : ${sample.mkString(", ")}")

    // ══════════════════════════════════════════════
    // 2. Actions numériques
    // ══════════════════════════════════════════════
    println("\n── 2. Actions numériques ──")

    println(s"count()    : ${rdd.count()}")
    println(s"sum()      : ${rdd.sum()}")
    println(s"min()      : ${rdd.min()}")
    println(s"max()      : ${rdd.max()}")
    println(f"mean()     : ${rdd.mean()}%.2f")
    println(f"stdev()    : ${rdd.stdev()}%.4f")
    println(f"variance() : ${rdd.variance()}%.4f")

    val stats = rdd.stats()
    println(s"\nstats() :\n  $stats")

    // ══════════════════════════════════════════════
    // 3. reduce() — agréger avec une fonction
    // ══════════════════════════════════════════════
    println("\n── 3. reduce() ──")

    val sumR:  Int = rddInt.reduce(_ + _)
    val prodR: Int = rddInt.reduce(_ * _)
    val maxR:  Int = rddInt.reduce(math.max)

    println(s"reduce(_ + _)       : $sumR")    // 55
    println(s"reduce(_ * _)       : $prodR")   // 3628800
    println(s"reduce(math.max)    : $maxR")    // 10

    // ══════════════════════════════════════════════
    // 4. fold() — avec valeur initiale
    // ══════════════════════════════════════════════
    println("\n── 4. fold() ──")

    val sumFold:  Int = rddInt.fold(0)(_ + _)   // 0 est l'élément neutre de +
    val prodFold: Int = rddInt.fold(1)(_ * _)   // 1 est l'élément neutre de *
    println(s"fold(0)(_ + _)      : $sumFold")
    println(s"fold(1)(_ * _)      : $prodFold")

    // ══════════════════════════════════════════════
    // 5. aggregate() — type de retour différent
    // ══════════════════════════════════════════════
    println("\n── 5. aggregate() — calculer moyenne ──")

    // Calculer (somme, count) en une seule passe
    val (sum5, count5) = rdd.aggregate((0.0, 0))(
      seqOp  = { case ((s, c), x) => (s + x, c + 1) },
      combOp = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    )
    val avg5 = sum5 / count5
    println(f"sum=$sum5, count=$count5, avg=$avg5%.2f")

    // Calculer min et max simultanément
    val (minVal, maxVal) = rdd.aggregate((Double.MaxValue, Double.MinValue))(
      seqOp  = { case ((mn, mx), x) => (math.min(mn, x), math.max(mx, x)) },
      combOp = { case ((mn1, mx1), (mn2, mx2)) => (math.min(mn1, mn2), math.max(mx1, mx2)) }
    )
    println(f"min=$minVal, max=$maxVal")

    // ══════════════════════════════════════════════
    // 6. countByValue() / countByKey()
    // ══════════════════════════════════════════════
    println("\n── 6. countByValue() / countByKey() ──")

    val countries = sc.parallelize(
      Seq("France", "UK", "France", "Germany", "UK", "France", "Germany")
    )
    val countsByValue: Map[String, Long] = countries.countByValue()
    println("countByValue :")
    countsByValue.toSeq.sortBy(-_._2).foreach { case (k, v) => println(s"  $k : $v") }

    val pairs = sc.parallelize(Seq(("a", 10), ("b", 20), ("a", 30), ("c", 40), ("b", 50)))
    val countsByKey: Map[String, Long] = pairs.countByKey()
    println("countByKey :")
    countsByKey.toSeq.sorted.foreach { case (k, v) => println(s"  $k : $v") }

    // ══════════════════════════════════════════════
    // 7. foreach() / foreachPartition()
    // ══════════════════════════════════════════════
    println("\n── 7. foreach / foreachPartition ──")

    // foreach — s'exécute sur les executors (println → logs executor, pas driver)
    println("foreach sur les 5 premiers éléments (via collect) :")
    rddInt.take(5).foreach(x => print(s"$x "))
    println()

    // foreachPartition — 1 initialisation par partition (pattern connexion DB)
    println("foreachPartition — simulation d'écriture batch :")
    rddInt.foreachPartition { iter =>
      val batch = iter.toList
      if (batch.nonEmpty)
        println(s"  Partition sur thread ${Thread.currentThread().getId} : ${batch.mkString(",")}")
    }

    // ══════════════════════════════════════════════
    // 8. saveAsTextFile
    // ══════════════════════════════════════════════
    println("\n── 8. saveAsTextFile() ──")

    val outputPath = "/tmp/spark_rdd_output"
    import java.io.File
    val outDir = new File(outputPath)
    if (outDir.exists()) outDir.listFiles().foreach(_.delete())

    val dataToSave = sc.parallelize(Seq(
      "France,5000", "UK,4000", "Germany,5500"
    ))
    dataToSave.saveAsTextFile(outputPath)
    println(s"Données sauvegardées dans : $outputPath")

    // Relire
    val relue = sc.textFile(outputPath)
    println("Contenu relu :")
    relue.collect().foreach(println)

    spark.stop()
    println("\nDémonstration Actions RDD terminée.")
  }
}
*/
package part4_rdd

/**
 * =============================================================
 *   Part 4 — 03 : Actions RDD
 *   collect, count, reduce, fold, aggregate, stats, save
 * =============================================================
 */

import org.apache.spark.sql.SparkSession

object RddActionsDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD_Actions_Demo")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    val rdd = sc.parallelize(
      Seq(3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0, 5.0, 3.0)
    )

    val rddInt = sc.parallelize(1 to 10)

    // ══════════════════════════════════════════════
    // 1. Actions de collecte
    // ══════════════════════════════════════════════
    println("\n── 1. Actions de collecte ──")

    val all: Array[Double] = rdd.collect()
    println(s"collect()           : ${all.mkString(", ")}")

    val first3: Array[Double] = rdd.take(3)
    println(s"take(3)             : ${first3.mkString(", ")}")

    val first: Double = rdd.first()
    println(s"first()             : $first")

    val top3: Array[Double] = rdd.top(3)
    println(s"top(3)              : ${top3.mkString(", ")}")

    val smallest3: Array[Double] = rdd.takeOrdered(3)
    println(s"takeOrdered(3)      : ${smallest3.mkString(", ")}")

    val sample: Array[Double] =
      rdd.takeSample(withReplacement = false, num = 4, seed = 42)

    println(s"takeSample(false,4) : ${sample.mkString(", ")}")

    // ══════════════════════════════════════════════
    // 2. Actions numériques
    // ══════════════════════════════════════════════
    println("\n── 2. Actions numériques ──")

    println(s"count()    : ${rdd.count()}")
    println(s"sum()      : ${rdd.sum()}")
    println(s"min()      : ${rdd.min()}")
    println(s"max()      : ${rdd.max()}")
    println(f"mean()     : ${rdd.mean()}%.2f")
    println(f"stdev()    : ${rdd.stdev()}%.4f")
    println(f"variance() : ${rdd.variance()}%.4f")

    val stats = rdd.stats()

    println(s"\nstats() :")
    println(s"  $stats")

    // ══════════════════════════════════════════════
    // 3. reduce() — agréger avec une fonction
    // ══════════════════════════════════════════════
    println("\n── 3. reduce() ──")

    val sumR: Int = rddInt.reduce(_ + _)
    val prodR: Int = rddInt.reduce(_ * _)
    val maxR: Int = rddInt.reduce(math.max)

    println(s"reduce(_ + _)       : $sumR")
    println(s"reduce(_ * _)       : $prodR")
    println(s"reduce(math.max)    : $maxR")

    // ══════════════════════════════════════════════
    // 4. fold() — avec valeur initiale
    // ══════════════════════════════════════════════
    println("\n── 4. fold() ──")

    val sumFold: Int = rddInt.fold(0)(_ + _)
    val prodFold: Int = rddInt.fold(1)(_ * _)

    println(s"fold(0)(_ + _)      : $sumFold")
    println(s"fold(1)(_ * _)      : $prodFold")

    // ══════════════════════════════════════════════
    // 5. aggregate() — type de retour différent
    // ══════════════════════════════════════════════
    println("\n── 5. aggregate() — calculer moyenne ──")

    // (somme, count)
    val (sum5, count5) = rdd.aggregate((0.0, 0))(
      seqOp = {
        case ((s, c), x) =>
          (s + x, c + 1)
      },
      combOp = {
        case ((s1, c1), (s2, c2)) =>
          (s1 + s2, c1 + c2)
      }
    )

    val avg5 = sum5 / count5

    println(f"sum=$sum5, count=$count5, avg=$avg5%.2f")

    // min + max en une seule passe
    val (minVal, maxVal) =
      rdd.aggregate((Double.MaxValue, Double.MinValue))(
        seqOp = {
          case ((mn, mx), x) =>
            (math.min(mn, x), math.max(mx, x))
        },
        combOp = {
          case ((mn1, mx1), (mn2, mx2)) =>
            (
              math.min(mn1, mn2),
              math.max(mx1, mx2)
            )
        }
      )

    println(f"min=$minVal, max=$maxVal")

    // ══════════════════════════════════════════════
    // 6. countByValue() / countByKey()
    // ══════════════════════════════════════════════
    println("\n── 6. countByValue() / countByKey() ──")

    val countries = sc.parallelize(
      Seq(
        "France",
        "UK",
        "France",
        "Germany",
        "UK",
        "France",
        "Germany"
      )
    )

    // Conversion vers immutable Map
    val countsByValue: Map[String, Long] =
      countries.countByValue().toMap

    println("countByValue :")

    countsByValue
      .toSeq
      .sortBy(-_._2)
      .foreach {
        case (k, v) =>
          println(s"  $k : $v")
      }

    val pairs = sc.parallelize(
      Seq(
        ("a", 10),
        ("b", 20),
        ("a", 30),
        ("c", 40),
        ("b", 50)
      )
    )

    // Conversion vers immutable Map
    val countsByKey: Map[String, Long] =
      pairs.countByKey().toMap

    println("countByKey :")

    countsByKey
      .toSeq
      .sorted
      .foreach {
        case (k, v) =>
          println(s"  $k : $v")
      }

    // ══════════════════════════════════════════════
    // 7. foreach() / foreachPartition()
    // ══════════════════════════════════════════════
    println("\n── 7. foreach / foreachPartition ──")

    println("foreach sur les 5 premiers éléments (via collect) :")

    rddInt.take(5).foreach(x => print(s"$x "))

    println()

    println("foreachPartition — simulation d'écriture batch :")

    rddInt.foreachPartition { iter =>

      val batch = iter.toList

      if (batch.nonEmpty) {
        println(
          s"  Partition sur thread ${Thread.currentThread().getId} : ${batch.mkString(",")}"
        )
      }
    }

    // ══════════════════════════════════════════════
    // 8. saveAsTextFile()
    // ══════════════════════════════════════════════
    println("\n── 8. saveAsTextFile() ──")

    val outputPath = "/tmp/spark_rdd_output"

    import java.io.File

    val outDir = new File(outputPath)

    if (outDir.exists()) {
      outDir.listFiles().foreach(_.delete())
      outDir.delete()
    }

    val dataToSave = sc.parallelize(
      Seq(
        "France,5000",
        "UK,4000",
        "Germany,5500"
      )
    )

    dataToSave.saveAsTextFile(outputPath)

    println(s"Données sauvegardées dans : $outputPath")

    // Relire les données
    val relue = sc.textFile(outputPath)

    println("Contenu relu :")

    relue.collect().foreach(println)

    spark.stop()

    println("\nDémonstration Actions RDD terminée.")
  }
}