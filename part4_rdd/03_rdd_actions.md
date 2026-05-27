# 03 — Actions RDD

Les actions **déclenchent l'exécution** du DAG et retournent un résultat au driver.

## Actions de collecte

```scala
val rdd = sc.parallelize(Seq(3, 1, 4, 1, 5, 9, 2, 6, 5, 3))

// collect() — ramène TOUS les éléments vers le driver (⚠️ dangereux sur gros datasets)
val all: Array[Int] = rdd.collect()
// → Array(3, 1, 4, 1, 5, 9, 2, 6, 5, 3)

// take(n) — ramène les n premiers éléments
val first3: Array[Int] = rdd.take(3)
// → Array(3, 1, 4)

// first() — ramène le premier élément
val first: Int = rdd.first()
// → 3

// top(n) — ramène les n plus grands éléments (ordre descendant)
val top3: Array[Int] = rdd.top(3)
// → Array(9, 6, 5)

// takeOrdered(n) — ramène les n plus petits éléments
val smallest3: Array[Int] = rdd.takeOrdered(3)
// → Array(1, 1, 2)

// takeSample(withReplacement, n, seed) — échantillon aléatoire
val sample: Array[Int] = rdd.takeSample(withReplacement = false, num = 4, seed = 42)
```

---

## Actions d'agrégation

```scala
val rdd = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0))

// count() — nombre d'éléments
val n: Long = rdd.count()
// → 5

// sum() — somme (sur RDD[Double] ou RDD[Float] ou RDD[Int])
val total: Double = rdd.sum()
// → 15.0

// min() / max()
val minimum: Double = rdd.min()  // → 1.0
val maximum: Double = rdd.max()  // → 5.0

// mean() — moyenne
val moyenne: Double = rdd.mean()  // → 3.0

// stdev() / variance()
val ecartType: Double = rdd.stdev()
val variance: Double  = rdd.variance()

// stats() — statistiques complètes en une passe
val stats = rdd.stats()
println(stats)
// (count: 5, mean: 3.0, stdev: 1.41, max: 5.0, min: 1.0)
```

---

## `reduce()` et `fold()`

```scala
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))

// reduce() — agréger tous les éléments avec une fonction associative
val sum: Int = rdd.reduce(_ + _)            // → 15
val product: Int = rdd.reduce(_ * _)        // → 120
val maxVal: Int = rdd.reduce(math.max)      // → 5

// fold() — comme reduce() avec une valeur initiale (zéro)
val sumFold: Int = rdd.fold(0)(_ + _)       // → 15
val product2: Int = rdd.fold(1)(_ * _)      // → 120
```

---

## `aggregate()` — Agrégation avec type de retour différent

```scala
val rdd = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0))

// aggregate(valeurInitiale)(seqOp, combOp)
// seqOp  : combine un accumulateur avec un élément   (dans chaque partition)
// combOp : combine deux accumulateurs                 (entre partitions)

val (sum, count) = rdd.aggregate((0.0, 0))(
  seqOp  = { case ((s, c), x) => (s + x, c + 1) },
  combOp = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
)
val avg = sum / count   // → 3.0
```

---

## Actions d'écriture

```scala
// saveAsTextFile() — écrire chaque élément comme une ligne texte
rdd.saveAsTextFile("output/rdd_text/")

// saveAsObjectFile() — sérialisation Java (pour relire avec objectFile())
rdd.saveAsObjectFile("output/rdd_object/")

// saveAsSequenceFile() — format Hadoop SequenceFile (PairRDD uniquement)
pairRdd.saveAsSequenceFile("output/rdd_sequence/")
```

---

## `foreach()` et `foreachPartition()`

```scala
// foreach() — appliquer une action sur chaque élément (sans retour)
rdd.foreach(x => println(x))   // s'exécute sur les executors !

// foreachPartition() — appliquer une action sur chaque partition
rdd.foreachPartition { iter =>
  // connexion DB ouverte UNE FOIS par partition
  val conn = openConnection()
  iter.foreach(x => conn.write(x))
  conn.close()
}
```

---

## `countByValue()` / `countByKey()`

```scala
val rdd = sc.parallelize(Seq("France", "UK", "France", "Germany", "UK", "France"))

// countByValue() — compte les occurrences de chaque valeur
val counts: Map[String, Long] = rdd.countByValue()
// → Map("France" -> 3, "UK" -> 2, "Germany" -> 1)

// countByKey() — pour les PairRDDs
val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3)))
val keyCount: Map[String, Long] = pairs.countByKey()
// → Map("a" -> 2, "b" -> 1)
```

---

## Tableau récapitulatif

| Action | Retour | Description |
|--------|--------|-------------|
| `collect()` | `Array[T]` | Tous les éléments vers le driver |
| `count()` | `Long` | Nombre d'éléments |
| `first()` | `T` | Premier élément |
| `take(n)` | `Array[T]` | n premiers éléments |
| `top(n)` | `Array[T]` | n plus grands éléments |
| `takeOrdered(n)` | `Array[T]` | n plus petits éléments |
| `reduce(f)` | `T` | Agrégation avec fonction |
| `fold(z)(f)` | `T` | Agrégation avec valeur initiale |
| `aggregate(z)(seq, comb)` | `U` | Agrégation type différent |
| `sum()` | `Double` | Somme |
| `mean()` | `Double` | Moyenne |
| `stats()` | `StatCounter` | Statistiques complètes |
| `countByValue()` | `Map[T, Long]` | Fréquences de chaque valeur |
| `saveAsTextFile(path)` | `Unit` | Écrire en fichiers texte |
| `foreach(f)` | `Unit` | Appliquer f sur chaque élément |

## Points clés pour l'examen

> ⚠️ `collect()` ramène **toutes** les données vers le driver — risque OOM sur gros datasets.

> ⚠️ `foreach()` s'exécute sur les **executors**, pas sur le driver — les `println` n'apparaissent pas dans la console driver !

> ✅ `foreachPartition()` est plus efficace que `foreach()` quand l'opération a un coût d'initialisation (connexion BD, etc.).

> ✅ `reduce()` nécessite une fonction **associative et commutative** (l'ordre d'évaluation entre partitions n'est pas garanti).
