# 04 — PairRDD & Partitionnement

## PairRDD — RDD de paires (clé, valeur)

Un **PairRDD** est un `RDD[(K, V)]`. Il expose des opérations spécialisées
clé-valeur absentes du RDD standard.

```scala
// Créer un PairRDD
val pairs = sc.parallelize(Seq(
  ("Alice",   5000.0),
  ("Bob",     4000.0),
  ("Alice",   6000.0),
  ("Charlie", 5500.0),
  ("Bob",     4200.0)
))

// Depuis un RDD standard avec map()
val rddLines = sc.textFile("employees.csv")
val pairRdd  = rddLines.map { line =>
  val cols = line.split(",")
  (cols(4), cols(3).toDouble)   // (country, salary)
}
```

---

## groupByKey() vs reduceByKey()

```scala
// groupByKey() — regroupe toutes les valeurs par clé
// ⚠️ Coûteux : transfère toutes les valeurs sur le réseau AVANT d'agréger
val grouped = pairs.groupByKey()
// → ("Alice",   Iterable(5000.0, 6000.0))
//   ("Bob",     Iterable(4000.0, 4200.0))
//   ("Charlie", Iterable(5500.0))

// Pour calculer la moyenne depuis groupByKey :
val avgFromGroup = grouped.mapValues(vals => vals.sum / vals.size)

// reduceByKey() — pré-agrège dans chaque partition AVANT le shuffle
// ✅ Beaucoup plus efficace
val totals = pairs.reduceByKey(_ + _)
// → ("Alice", 11000.0), ("Bob", 8200.0), ("Charlie", 5500.0)

// Pour la moyenne avec reduceByKey, combiner somme + count :
val sumCount = pairs.mapValues(v => (v, 1))
  .reduceByKey { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
val avgRed = sumCount.mapValues { case (s, c) => s / c }
```

---

## aggregateByKey()

```scala
// aggregateByKey(valeurInitiale)(seqOp, combOp)
// La valeur initiale peut avoir un TYPE DIFFÉRENT des valeurs du RDD

val stats = pairs.aggregateByKey((0.0, 0))(
  // seqOp : combine accumulateur + valeur (dans chaque partition)
  seqOp  = { case ((sum, count), salary) => (sum + salary, count + 1) },
  // combOp : combine deux accumulateurs (entre partitions)
  combOp = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
)
// → ("Alice", (11000.0, 2)), ("Bob", (8200.0, 2)), ("Charlie", (5500.0, 1))

val avg = stats.mapValues { case (sum, count) => sum / count }
// → ("Alice", 5500.0), ("Bob", 4100.0), ("Charlie", 5500.0)
```

---

## combineByKey() — Le plus flexible

```scala
// combineByKey(createCombiner, mergeValue, mergeCombiners)
// Permet de contrôler chaque étape de l'agrégation

val result = pairs.combineByKey(
  createCombiner  = (v: Double) => (v, 1),                              // 1er élément de la clé
  mergeValue      = (acc: (Double, Int), v: Double) => (acc._1 + v, acc._2 + 1),  // ajout dans partition
  mergeCombiners  = (a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2) // fusion partitions
)
val avgCombine = result.mapValues { case (sum, cnt) => sum / cnt }
```

---

## Partitionnement des PairRDDs

Le partitionnement contrôle sur quel executor chaque clé est envoyée.

```scala
// HashPartitioner — partitionnement par hash de clé (défaut après groupByKey, reduceByKey)
import org.apache.spark.HashPartitioner

val partitioned = pairs.partitionBy(new HashPartitioner(4))
println(partitioned.partitioner)   // → Some(HashPartitioner(4))
println(partitioned.getNumPartitions)  // → 4

// RangePartitioner — partitionnement par plage de valeurs (après sortByKey)
import org.apache.spark.RangePartitioner

val sorted = pairs.sortByKey()
println(sorted.partitioner)   // → Some(RangePartitioner)
```

---

## Avantage du partitionnement persisté

```scala
// Sans partitionnement persisté — shuffle à chaque jointure
val result1 = pairs.join(otherPairs)   // shuffle !
val result2 = pairs.join(anotherPairs) // shuffle encore !

// Avec partitionnement persisté — le shuffle n'a lieu qu'une fois
val partitionedPairs = pairs.partitionBy(new HashPartitioner(4)).persist()

val result1 = partitionedPairs.join(otherPairs)   // shuffle côté otherPairs seulement
val result2 = partitionedPairs.join(anotherPairs) // idem — partitionedPairs déjà en place
```

---

## mapValues() vs map() — Préserver le partitionnement

```scala
val partitioned = pairs.partitionBy(new HashPartitioner(4)).persist()

// map() — PERD le partitionnement (crée un nouveau RDD sans partitioner)
val mapped = partitioned.map { case (k, v) => (k, v * 2) }
println(mapped.partitioner)   // → None  ⚠️

// mapValues() — CONSERVE le partitionnement
val mapped2 = partitioned.mapValues(v => v * 2)
println(mapped2.partitioner)  // → Some(HashPartitioner(4))  ✅
```

---

## Points clés pour l'examen

> ⚠️ `groupByKey()` → shuffle de toutes les valeurs. `reduceByKey()` → pré-agrégation locale puis shuffle. **Toujours préférer `reduceByKey()`** quand possible.

> ✅ `aggregateByKey()` permet d'agréger vers un **type de sortie différent** du type d'entrée.

> ✅ `mapValues()` préserve le partitionnement ; `map()` le détruit.

> ✅ Après `partitionBy(new HashPartitioner(n)).persist()`, les jointures successives sur la même clé ne génèrent **qu'un seul shuffle** (côté l'autre RDD).
