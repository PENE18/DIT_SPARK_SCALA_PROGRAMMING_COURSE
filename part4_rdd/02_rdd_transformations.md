# 02 — Transformations RDD

Les transformations RDD sont **lazy** — elles construisent le plan sans exécuter de calcul.

## Transformations sur éléments

```scala
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

// map() — transformer chaque élément
val doubled = rdd.map(x => x * 2)
// → [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

// flatMap() — transformer et aplatir (1 élément → N éléments)
val words = sc.parallelize(Seq("hello world", "foo bar"))
val wordRdd = words.flatMap(line => line.split(" "))
// → ["hello", "world", "foo", "bar"]

// filter() — garder les éléments qui satisfont le prédicat
val evens = rdd.filter(x => x % 2 == 0)
// → [2, 4, 6, 8, 10]

// mapPartitions() — transformer par partition (plus efficace que map())
val result = rdd.mapPartitions(iter => iter.map(x => x * 2))

// mapPartitionsWithIndex() — avec l'index de partition
val withIdx = rdd.mapPartitionsWithIndex { (idx, iter) =>
  iter.map(x => s"partition=$idx, value=$x")
}
```

---

## Transformations d'ensemble

```scala
val rdd1 = sc.parallelize(Seq(1, 2, 3, 4))
val rdd2 = sc.parallelize(Seq(3, 4, 5, 6))

// union() — fusionner deux RDDs (garde les doublons)
val unionRdd = rdd1.union(rdd2)
// → [1, 2, 3, 4, 3, 4, 5, 6]

// intersection() — éléments communs aux deux RDDs
val intersect = rdd1.intersection(rdd2)
// → [3, 4]

// subtract() — éléments dans rdd1 mais pas dans rdd2
val diff = rdd1.subtract(rdd2)
// → [1, 2]

// distinct() — supprimer les doublons
val distinct = unionRdd.distinct()
// → [1, 2, 3, 4, 5, 6]

// cartesian() — produit cartésien (N × M éléments)
val cart = rdd1.cartesian(rdd2)
// → [(1,3), (1,4), (1,5), (1,6), (2,3), ...]
```

---

## Transformations de partitionnement

```scala
// repartition() — redistribuer sur N partitions (shuffle complet)
val repartitioned = rdd.repartition(4)

// coalesce() — réduire les partitions (sans shuffle complet)
val coalesced = rdd.coalesce(2)

// glom() — regrouper chaque partition en un tableau
val partitionArrays = rdd.glom()
// RDD[Array[Int]] — un Array par partition
```

---

## Transformations sur RDDs de paires (clé-valeur)

Les **PairRDDs** (`RDD[(K, V)]`) exposent des transformations supplémentaires.

```scala
val pairs = sc.parallelize(Seq(
  ("France", 5000.0), ("UK", 4000.0),
  ("France", 6000.0), ("Germany", 5500.0), ("UK", 4500.0)
))

// mapValues() — transformer seulement les valeurs
val doubled = pairs.mapValues(v => v * 2)

// flatMapValues() — transformer la valeur en plusieurs valeurs
val expanded = pairs.flatMapValues(v => Seq(v, v * 1.1))

// groupByKey() — regrouper les valeurs par clé
val grouped = pairs.groupByKey()
// → ("France", [5000.0, 6000.0]), ("UK", [4000.0, 4500.0]), ...

// reduceByKey() — agréger les valeurs par clé (plus efficace que groupByKey)
val totals = pairs.reduceByKey(_ + _)
// → ("France", 11000.0), ("UK", 8500.0), ("Germany", 5500.0)

// aggregateByKey() — agréger avec valeur initiale différente
val stats = pairs.aggregateByKey((0.0, 0))(
  seqOp   = { case ((sum, cnt), v) => (sum + v, cnt + 1) },  // dans une partition
  combOp  = { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }  // entre partitions
)
val avg = stats.mapValues { case (sum, cnt) => sum / cnt }

// sortByKey() — trier par clé
val sorted    = pairs.sortByKey()                 // croissant
val sortedDesc = pairs.sortByKey(ascending = false) // décroissant

// keys() / values() — extraire clés ou valeurs
val keys   = pairs.keys    // RDD[String]
val values = pairs.values  // RDD[Double]
```

---

## Jointures sur PairRDDs

```scala
val employees = sc.parallelize(Seq(
  (1, "Alice"), (2, "Bob"), (3, "Carol")
))
val departments = sc.parallelize(Seq(
  (1, "Engineering"), (2, "Sales"), (4, "HR")
))

// join() — inner join
val inner = employees.join(departments)
// → (1, ("Alice", "Engineering")), (2, ("Bob", "Sales"))

// leftOuterJoin()
val left = employees.leftOuterJoin(departments)
// → (1, ("Alice", Some("Engineering"))),
//   (2, ("Bob",   Some("Sales"))),
//   (3, ("Carol", None))

// rightOuterJoin()
val right = employees.rightOuterJoin(departments)
// → (1, (Some("Alice"), "Engineering")),
//   (2, (Some("Bob"),   "Sales")),
//   (4, (None,          "HR"))

// fullOuterJoin()
val full = employees.fullOuterJoin(departments)

// cogroup() — regrouper les deux RDDs par clé
val cogrouped = employees.cogroup(departments)
// → (1, ([Alice], [Engineering])), (2, ([Bob], [Sales])), ...
```

---

## `sortBy()` — Tri général

```scala
val rdd = sc.parallelize(Seq(("b", 2), ("a", 5), ("c", 1)))

// Trier par valeur
val sortedByVal = rdd.sortBy(_._2)                  // croissant
val sortedByValDesc = rdd.sortBy(_._2, ascending = false) // décroissant

// Trier par clé
val sortedByKey = rdd.sortBy(_._1)
```

---

## `zip()` et `zipWithIndex()`

```scala
val rddA = sc.parallelize(Seq("Alice", "Bob", "Carol"))
val rddB = sc.parallelize(Seq(5000.0, 4000.0, 6000.0))

// zip() — combiner deux RDDs élément par élément (même nombre de partitions/éléments)
val zipped = rddA.zip(rddB)
// → ("Alice", 5000.0), ("Bob", 4000.0), ("Carol", 6000.0)

// zipWithIndex() — ajouter un index
val withIdx = rddA.zipWithIndex()
// → ("Alice", 0), ("Bob", 1), ("Carol", 2)
```

---

## Tableau récapitulatif

| Transformation | Type | Description |
|----------------|------|-------------|
| `map(f)` | Élément | 1 → 1 |
| `flatMap(f)` | Élément | 1 → N (aplatit) |
| `filter(f)` | Élément | Filtre par prédicat |
| `mapPartitions(f)` | Partition | Transforme par partition |
| `groupByKey()` | PairRDD | Regroupe valeurs par clé |
| `reduceByKey(f)` | PairRDD | Agrège valeurs par clé |
| `aggregateByKey(z)(seq, comb)` | PairRDD | Agrégation flexible |
| `sortByKey()` | PairRDD | Trie par clé |
| `join()` | PairRDD | Inner join |
| `leftOuterJoin()` | PairRDD | Left join |
| `union()` | RDD | Fusion (avec doublons) |
| `distinct()` | RDD | Suppression doublons |
| `repartition(n)` | RDD | Redistribution (shuffle) |
| `coalesce(n)` | RDD | Réduction (sans shuffle) |

## Points clés pour l'examen

> ⚠️ `groupByKey()` transfère **toutes** les valeurs sur le réseau avant d'agréger → préférer `reduceByKey()` qui fait une pré-agrégation locale.

> ✅ `mapPartitions()` est plus efficace que `map()` quand l'opération a un coût d'initialisation (ex: connexion DB, parsing complexe).

> ✅ `flatMap()` est indispensable pour le **word count** et toute opération 1-à-N.
