# 07 — Transformations vs Actions

## Principe de l'évaluation lazy

Spark est **lazy** : les transformations ne s'exécutent pas immédiatement.
Elles construisent un plan (DAG). L'exécution ne démarre qu'au moment d'une **action**.

```scala
val df  = spark.read.option("header", "true").csv("data.csv")  // transformation (lazy)
val df2 = df.filter(col("age") > 30)                           // transformation (lazy)
val df3 = df2.groupBy("country").count()                       // transformation (lazy)
df3.show()                                                      // ACTION → exécution réelle
```

> [OK] **Réponse d'examen :** L'exécution démarre à la **ligne 4** (`show()` est une action).

---

## Transformations (Lazy — construisent le plan)

| Transformation | Description |
|----------------|-------------|
| `filter()` / `where()` | Filtrage des lignes |
| `select()` | Sélection/projection de colonnes |
| `withColumn()` | Ajouter ou modifier une colonne |
| `groupBy()` | Groupement pour agrégation |
| `join()` | Jointure entre deux DataFrames |
| `orderBy()` / `sort()` | Tri |
| `distinct()` | Suppression des doublons |
| `limit(n)` | Prendre les n premières lignes |
| `union()` | Combiner deux DataFrames |
| `drop()` | Supprimer des colonnes |
| `repartition(n)` | Redistribuer en n partitions (shuffle complet) |
| `coalesce(n)` | Réduire les partitions (sans shuffle complet) |
| `cache()` / `persist()` | Marquer pour mise en cache |

---

## Actions (Eager — déclenchent l'exécution)

| Action | Description |
|--------|-------------|
| `show(n)` | Affiche n lignes |
| `collect()` | Retourne toutes les lignes comme `Array[Row]` |
| `count()` | Compte le nombre total de lignes |
| `first()` / `head()` | Retourne la première ligne |
| `take(n)` | Retourne les n premières lignes comme `Array[Row]` |
| `write.csv()` | Écrit en CSV |
| `write.parquet()` | Écrit en Parquet |
| `describe()` | Statistiques descriptives |

> [INFO] En Scala, `collect()` retourne un `Array[Row]` (pas une liste Python).

---

## Points clés pour l'examen

> [ATTENTION] `cache()` est aussi **lazy** — la mise en cache ne se produit qu'au moment de la première **action** après `cache()`.

> [ATTENTION] `collect()` ramène **toutes** les données vers le driver — dangereux sur de gros datasets !

> [OK] Enchaîner des transformations est gratuit (pas de calcul). Seule l'action déclenche tout.
