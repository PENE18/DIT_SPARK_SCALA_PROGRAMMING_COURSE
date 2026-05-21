# 02 — Repartition vs Coalesce

## Pourquoi le partitionnement est important

Spark divise les données en **partitions** traitées en parallèle sur les executors.
Le nombre de partitions affecte directement le parallélisme et le coût des shuffles.

```scala
// Vérifier le nombre de partitions
df.rdd.getNumPartitions

// Partitions de shuffle (défaut : 200)
spark.conf.get("spark.sql.shuffle.partitions")
```

## `repartition(n)` — Shuffle complet

Redistribue **uniformément** les données sur `n` partitions via un shuffle complet.

```scala
val dfRep1 = df.repartition(10)
val dfRep2 = df.repartition(col("country"))                  // par colonne
val dfRep3 = df.repartition(10, col("country"))              // n + colonne
val dfRep4 = df.repartition(10, col("country"), col("dept")) // multi-colonnes
```

**Quand utiliser `repartition()` :**
- Augmenter le nombre de partitions
- Distribuer uniformément des données skewées
- Optimiser pour les jointures (co-partitionner par la clé de jointure)
- Contrôler le nombre de fichiers de sortie

## `coalesce(n)` — Sans shuffle complet

Réduit les partitions **sans shuffle complet** en fusionnant des partitions adjacentes.

```scala
val dfCoal = df.coalesce(2)
```

**Quand utiliser `coalesce()` :**
- Réduire les partitions (notamment avant écriture pour éviter les petits fichiers)
- Après un filtre agressif (beaucoup moins de lignes → trop de petites partitions)
- Chemin critique en performance (évite le coût du shuffle)

## Différences clés

| Fonctionnalité | `repartition(n)` | `coalesce(n)` |
|----------------|------------------|---------------|
| Shuffle | [OK] Complet | [NON] Pas de shuffle complet |
| Direction | Augmenter OU diminuer | Diminuer uniquement |
| Distribution | Uniforme | Potentiellement inégale |
| Coût | Élevé | Faible |
| Par colonne | [OK] Oui | [NON] Non |

## Exemples pratiques

```scala
// Problème : trop de petits fichiers de sortie
df.coalesce(5).write.parquet("/mnt/output/")   // 5 fichiers au lieu de 200

// Optimiser une jointure (co-partitionner les deux côtés)
val dfOrders    = orders.repartition(200, col("customer_id"))
val dfCustomers = customers.repartition(200, col("customer_id"))
val result      = dfOrders.join(dfCustomers, "customer_id")

// Après un filtre très sélectif
val dfErreurs = df.filter(col("statut") === "ERREUR")  // 0.1% des données
val dfCompact = dfErreurs.coalesce(4)                   // compacter
```

## Points clés pour l'examen

> [ATTENTION] `coalesce(1)` ramène toutes les données vers un seul executor — dangereux pour les gros datasets !

> [OK] `repartition()` peut augmenter OU diminuer les partitions. `coalesce()` ne peut que **diminuer**.

> [OK] Préférer `coalesce()` quand on veut juste réduire les partitions — moins cher que `repartition()`.
