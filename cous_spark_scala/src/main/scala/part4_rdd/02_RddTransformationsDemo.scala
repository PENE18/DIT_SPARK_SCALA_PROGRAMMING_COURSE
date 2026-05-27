package part4_rdd

/**
 * =============================================================
 *   Part 4 — 02 : Transformations RDD
 *   map, flatMap, filter, mapPartitions, set ops, PairRDD
 * =============================================================
 */

import org.apache.spark.sql.SparkSession

object RddTransformationsDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD_Transformations_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    // ══════════════════════════════════════════════
    // 1. map() — transformer chaque élément
    // ══════════════════════════════════════════════
    println("\n── 1. map() ──")

    val nums = sc.parallelize(1 to 10)
    val squared = nums.map(x => x * x)
    println(s"Carrés : ${squared.collect().mkString(", ")}")

    val words = sc.parallelize(Seq("bonjour", "monde", "spark", "scala"))
    val upper = words.map(_.toUpperCase)
    println(s"Majuscules : ${upper.collect().mkString(", ")}")

    // ══════════════════════════════════════════════
    // 2. flatMap() — 1 élément → N éléments
    // ══════════════════════════════════════════════
    println("\n── 2. flatMap() ──")

    val sentences = sc.parallelize(Seq(
      "Spark est rapide",
      "Scala est expressif",
      "Les RDDs sont puissants"
    ))
    val allWords = sentences.flatMap(_.split(" "))
    println(s"Tous les mots : ${allWords.collect().mkString(", ")}")
    println(s"Nombre de mots : ${allWords.count()}")

    // ══════════════════════════════════════════════
    // 3. filter() — garder les éléments qui passent le prédicat
    // ══════════════════════════════════════════════
    println("\n── 3. filter() ──")

    val evens  = nums.filter(_ % 2 == 0)
    val odds   = nums.filter(_ % 2 != 0)
    println(s"Pairs    : ${evens.collect().mkString(", ")}")
    println(s"Impairs  : ${odds.collect().mkString(", ")}")

    // ══════════════════════════════════════════════
    // 4. mapPartitions() — transformer par partition
    // ══════════════════════════════════════════════
    println("\n── 4. mapPartitions() — plus efficace pour initialisation coûteuse ──")

    val rdd4parts = sc.parallelize(1 to 20, numSlices = 4)

    val partResult = rdd4parts.mapPartitions { iter =>
      // L'initialisation se fait UNE FOIS par partition
      val prefix = s"[P${Thread.currentThread().getId}]"
      iter.map(x => s"$prefix:$x")
    }
    println(s"Résultat mapPartitions (5 premiers) : ${partResult.take(5).mkString(", ")}")

    // mapPartitionsWithIndex — avec l'index de partition
    val withIdx = rdd4parts.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(x => s"p$idx->$x")
    }
    println(s"Avec index : ${withIdx.take(8).mkString(", ")}")

    // ══════════════════════════════════════════════
    // 5. Opérations d'ensemble
    // ══════════════════════════════════════════════
    println("\n── 5. union / intersection / subtract / distinct ──")

    val rddA = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rddB = sc.parallelize(Seq(3, 4, 5, 6, 7))

    println(s"union()        : ${rddA.union(rddB).collect().sorted.mkString(", ")}")
    println(s"intersection() : ${rddA.intersection(rddB).collect().sorted.mkString(", ")}")
    println(s"subtract()     : ${rddA.subtract(rddB).collect().sorted.mkString(", ")}")
    println(s"distinct()     : ${rddA.union(rddB).distinct().collect().sorted.mkString(", ")}")

    // ══════════════════════════════════════════════
    // 6. PairRDD — map, groupByKey, reduceByKey
    // ══════════════════════════════════════════════
    println("\n── 6. PairRDD ──")

    val pairs = sc.parallelize(Seq(
      ("France",  5000.0), ("UK",      4000.0),
      ("France",  6000.0), ("Germany", 5500.0),
      ("UK",      4200.0), ("France",  5500.0)
    ))

    // mapValues — transforme seulement les valeurs
    val taxed = pairs.mapValues(v => v * 0.8)
    println("mapValues (salaire net) :")
    taxed.collect().foreach { case (k, v) => println(s"  $k : $v") }

    // groupByKey — ⚠️ Coûteux
    println("\ngroupByKey :")
    pairs.groupByKey().collect().foreach { case (k, vs) =>
      println(s"  $k : [${vs.mkString(", ")}]  avg=${vs.sum / vs.size}")
    }

    // reduceByKey — ✅ Efficace (pré-agrégation locale)
    println("\nreduceByKey (somme) :")
    pairs.reduceByKey(_ + _).collect().foreach { case (k, v) =>
      println(s"  $k : $v")
    }

    // sortByKey — trier par clé
    println("\nsortByKey :")
    pairs.reduceByKey(_ + _).sortByKey().collect().foreach { case (k, v) =>
      println(s"  $k : $v")
    }

    // keys / values
    println(s"\nkeys()   : ${pairs.keys.distinct().collect().sorted.mkString(", ")}")
    println(s"values() : ${pairs.values.collect().mkString(", ")}")

    // ══════════════════════════════════════════════
    // 7. aggregateByKey — type de retour différent
    // ══════════════════════════════════════════════
    println("\n── 7. aggregateByKey() — (somme, count) ──")

    val stats = pairs.aggregateByKey((0.0, 0))(
      seqOp  = { case ((sum, cnt), v) => (sum + v, cnt + 1) },
      combOp = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    )
    val avgByCountry = stats.mapValues { case (sum, cnt) => sum / cnt }
    println("Salaire moyen par pays :")
    avgByCountry.collect().foreach { case (k, avg) =>
      println(f"  $k : $avg%.2f")
    }

    // ══════════════════════════════════════════════
    // 8. Jointures PairRDD
    // ══════════════════════════════════════════════
    println("\n── 8. Jointures PairRDD ──")

    val employees = sc.parallelize(Seq(
      (1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave")
    ))
    val departments = sc.parallelize(Seq(
      (1, "Engineering"), (2, "Sales"), (5, "HR")
    ))

    println("join (inner) :")
    employees.join(departments).collect().foreach(println)

    println("\nleftOuterJoin :")
    employees.leftOuterJoin(departments).collect().foreach(println)

    println("\nrightOuterJoin :")
    employees.rightOuterJoin(departments).collect().foreach(println)

    // ══════════════════════════════════════════════
    // 9. zip et zipWithIndex
    // ══════════════════════════════════════════════
    println("\n── 9. zip / zipWithIndex ──")

    val names    = sc.parallelize(Seq("Alice", "Bob", "Carol"))
    val salaries = sc.parallelize(Seq(5000.0, 4000.0, 6000.0))

    names.zip(salaries).collect().foreach { case (n, s) =>
      println(s"  $n : $s")
    }

    names.zipWithIndex().collect().foreach { case (n, idx) =>
      println(s"  [$idx] $n")
    }

    // ══════════════════════════════════════════════
    // 10. sortBy
    // ══════════════════════════════════════════════
    println("\n── 10. sortBy() ──")

    val data = sc.parallelize(Seq(("b", 2), ("a", 5), ("c", 1), ("d", 3)))
    println("Trié par valeur (asc)  : " + data.sortBy(_._2).collect().mkString(", "))
    println("Trié par valeur (desc) : " + data.sortBy(_._2, ascending = false).collect().mkString(", "))
    println("Trié par clé           : " + data.sortBy(_._1).collect().mkString(", "))

    spark.stop()
    println("\nDémonstration Transformations RDD terminée.")
  }
}
