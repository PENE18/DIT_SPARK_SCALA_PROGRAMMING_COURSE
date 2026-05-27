# 01 — Introduction aux RDDs

## Qu'est-ce qu'un RDD ?

Le **RDD** (Resilient Distributed Dataset) est l'abstraction fondamentale de Spark.
C'est une collection d'éléments **immuable**, **distribuée** et **tolérante aux pannes**,
partitionnée sur les nœuds du cluster.

```
RDD = Resilient   → tolérant aux pannes (reconstruit depuis le lineage)
      Distributed → distribué sur plusieurs nœuds
      Dataset     → collection de données typée
```

---

## RDD vs DataFrame

| Critère | RDD | DataFrame |
|---------|-----|-----------|
| API | Bas niveau (fonctionnelle) | Haut niveau (SQL-like) |
| Typage | Fortement typé (`RDD[T]`) | Schema dynamique |
| Optimisation | ❌ Manuelle | ✅ Catalyst + Tungsten auto |
| Performance | Moins rapide (sans code gen) | Plus rapide |
| Cas d'usage | Contrôle fin, logique complexe | ETL, SQL, ML standard |
| Sérialisation | Java / Kryo | Encodeurs Spark (plus efficaces) |

> ✅ **Règle** : Utiliser les DataFrames par défaut. Recourir aux RDDs uniquement quand
> le contrôle bas niveau est indispensable (algorithmes custom, manipulation binaire, etc.).

---

## Créer un RDD

```scala
val sc = spark.sparkContext

// 1. Depuis une collection en mémoire
val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
val rdd2 = sc.parallelize(Seq(1, 2, 3, 4, 5), numSlices = 4)  // 4 partitions

// 2. Depuis un fichier texte (chaque ligne = un élément)
val rddLines = sc.textFile("data/csv/employees.csv")
val rddLines = sc.textFile("data/csv/employees.csv", minPartitions = 4)

// 3. Depuis plusieurs fichiers
val rddMulti = sc.textFile("data/*.csv")

// 4. Depuis un dossier entier
val rddDir = sc.wholeTextFiles("data/csv/")  // RDD[(nomFichier, contenu)]

// 5. Depuis un DataFrame
val rdd = df.rdd                            // RDD[Row]
val rddTyped = df.as[MyCaseClass].rdd       // RDD[MyCaseClass]
```

---

## Propriétés clés d'un RDD

```scala
// Nombre de partitions
rdd.getNumPartitions

// Lineage (DAG de dépendances)
rdd.toDebugString

// Schéma de partitionnement (si défini)
rdd.partitioner
```

---

## Le Lineage — Tolérance aux pannes

Spark ne réplique pas les données. Il mémorise le **lineage** (la chaîne de transformations)
pour reconstruire les partitions perdues en cas de panne d'un executor.

```
sc.textFile("data.txt")         ← RDD de base (depuis disque)
  .filter(_.contains("France")) ← RDD transformé
  .map(_.split(","))            ← RDD transformé
  .map(_(0))                    ← RDD transformé
         ↑
  Si une partition est perdue → Spark rejoue ces 3 étapes depuis le fichier source
```

---

## Points clés pour l'examen

> ✅ Un RDD est **immuable** — chaque transformation crée un **nouveau** RDD.

> ✅ Les RDDs sont **lazy** — les transformations ne s'exécutent qu'au déclenchement d'une action.

> ✅ La tolérance aux pannes passe par le **lineage**, pas par la réplication des données.

> ⚠️ Les RDDs ne bénéficient pas de l'optimiseur Catalyst — les performances sont inférieures aux DataFrames pour les opérations SQL classiques.
