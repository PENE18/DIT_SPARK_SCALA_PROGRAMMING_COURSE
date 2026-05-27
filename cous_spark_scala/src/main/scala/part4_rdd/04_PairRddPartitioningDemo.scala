package part4_rdd

/**
 * =============================================================
 *   Part 4 — 04 : PairRDD & Partitionnement
 *   groupByKey vs reduceByKey, aggregateByKey, combineByKey,
 *   HashPartitioner, RangePartitioner, mapValues
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

object PairRddPartitioningDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PairRDD_Partitioning_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    val pairs = sc.parallelize(Seq(
      ("France",  5000.0), ("UK",      4000.0),
      ("France",  6000.0), ("Germany", 5500.0),
      ("UK",      4200.0), ("France",  5500.0),
      ("Germany", 6200.0), ("UK",      3800.0)
    ))

    // ══════════════════════════════════════════════
    // 1. groupByKey() — ⚠️ Coûteux
    // ══════════════════════════════════════════════
    println("\n── 1. groupByKey() ──")

    val grouped = pairs.groupByKey()
    println("Résultat :")
    grouped.collect().foreach { case (country, salaries) =>
      val lst = salaries.toList
      println(f"  $country%-10s : [${lst.map(s => f"$s%.0f").mkString(", ")}]  avg=${lst.sum/lst.size}%.0f")
    }

    // ══════════════════════════════════════════════
    // 2. reduceByKey() — ✅ Efficace
    // ══════════════════════════════════════════════
    println("\n── 2. reduceByKey() — pré-agrégation locale ──")

    // Somme par pays
    val totalsByCountry = pairs.reduceByKey(_ + _)
    println("Somme des salaires par pays :")
    totalsByCountry.sortByKey().collect().foreach { case (k, v) =>
      println(f"  $k%-10s : $v%.0f")
    }

    // Max par pays
    val maxByCountry = pairs.reduceByKey(math.max)
    println("Max salaire par pays :")
    maxByCountry.sortByKey().collect().foreach { case (k, v) =>
      println(f"  $k%-10s : $v%.0f")
    }

    // ══════════════════════════════════════════════
    // 3. aggregateByKey() — type de retour différent
    // ══════════════════════════════════════════════
    println("\n── 3. aggregateByKey() — moyenne ──")

    val statsRdd = pairs.aggregateByKey((0.0, 0))(
      seqOp  = { case ((sum, cnt), v) => (sum + v, cnt + 1) },
      combOp = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    )

    println("(somme, count, moyenne) par pays :")
    statsRdd.sortByKey().collect().foreach { case (k, (sum, cnt)) =>
      println(f"  $k%-10s : sum=$sum%.0f  count=$cnt  avg=${sum/cnt}%.2f")
    }

    // ══════════════════════════════════════════════
    // 4. combineByKey() — contrôle total
    // ══════════════════════════════════════════════
    println("\n── 4. combineByKey() ──")

    val combined = pairs.combineByKey(
      createCombiner  = (v: Double) => (v, 1),                         // 1er élément
      mergeValue      = (acc: (Double, Int), v: Double) => (acc._1 + v, acc._2 + 1),
      mergeCombiners  = (a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2)
    )

    println("Résultat combineByKey (identique à aggregateByKey) :")
    combined.mapValues { case (sum, cnt) => sum / cnt }
      .sortByKey()
      .collect()
      .foreach { case (k, avg) => println(f"  $k%-10s : avg=$avg%.2f") }

    // ══════════════════════════════════════════════
    // 5. mapValues() vs map() — impact sur partitioner
    // ══════════════════════════════════════════════
    println("\n── 5. mapValues() conserve le partitioner ──")

    val partitioned = pairs.partitionBy(new HashPartitioner(3)).persist()
    println(s"Partitioner original   : ${partitioned.partitioner}")

    val viaMV  = partitioned.mapValues(_ * 1.1)
    val viaMap = partitioned.map { case (k, v) => (k, v * 1.1) }

    println(s"Après mapValues()  partitioner : ${viaMV.partitioner}")   // ✅ conservé
    println(s"Après map()        partitioner : ${viaMap.partitioner}")   // ❌ perdu (None)

    // ══════════════════════════════════════════════
    // 6. HashPartitioner — partitionnement par hash
    // ══════════════════════════════════════════════
    println("\n── 6. HashPartitioner(4) ──")

    val hashPart = pairs.partitionBy(new HashPartitioner(4)).persist()
    println(s"Partitions : ${hashPart.getNumPartitions}")

    println("Distribution par partition :")
    hashPart.mapPartitionsWithIndex { (idx, iter) =>
      val elems = iter.toList
      Iterator(s"  Partition $idx : ${elems.map(_._1).mkString(", ")} (${elems.size} éléments)")
    }.collect().foreach(println)

    // ══════════════════════════════════════════════
    // 7. RangePartitioner — partitionnement par plage
    // ══════════════════════════════════════════════
    println("\n── 7. RangePartitioner (après sortByKey) ──")

    val sorted = pairs.sortByKey()
    println(s"Partitioner après sortByKey : ${sorted.partitioner}")

    val rp = new RangePartitioner(3, pairs)
    val rangePart = pairs.partitionBy(rp)
    println(s"Partitioner RangePartitioner(3) : ${rangePart.partitioner}")

    println("Distribution :")
    rangePart.mapPartitionsWithIndex { (idx, iter) =>
      val elems = iter.toList
      Iterator(s"  Partition $idx : ${elems.map(_._1).mkString(", ")}")
    }.collect().foreach(println)

    // ══════════════════════════════════════════════
    // 8. Avantage du partitionnement persisté pour les jointures
    // ══════════════════════════════════════════════
    println("\n── 8. Partitionnement persisté — optimiser les jointures multiples ──")

    val employees = sc.parallelize(Seq(
      ("France",  "Alice"),  ("UK",      "Bob"),
      ("France",  "Carol"),  ("Germany", "Dave"),
      ("UK",      "Eve")
    )).partitionBy(new HashPartitioner(3)).persist()

    val capitals = sc.parallelize(Seq(
      ("France",  "Paris"),  ("UK",      "London"), ("Germany", "Berlin")
    ))
    val currencies = sc.parallelize(Seq(
      ("France",  "EUR"),    ("UK",      "GBP"),    ("Germany", "EUR")
    ))

    // Jointures successives — employees déjà partitionné → shuffle minimal
    val withCapital   = employees.join(capitals)
    val withCurrency  = employees.join(currencies)

    println("Employés avec capitale :")
    withCapital.collect().foreach { case (country, (emp, cap)) =>
      println(s"  $country : $emp vit à $cap")
    }

    println("\nEmployés avec devise :")
    withCurrency.collect().foreach { case (country, (emp, curr)) =>
      println(s"  $country : $emp utilise $curr")
    }

    employees.unpersist()
    hashPart.unpersist()
    partitioned.unpersist()

    spark.stop()
    println("\nDémonstration PairRDD & Partitionnement terminée.")
  }
}
