# 06 — Union & UnionAll

## `union()` vs `unionAll()`

| Méthode | Supprime les doublons ? | Version Spark |
|---------|------------------------|---------------|
| `union()` | [NON] Non (garde toutes les lignes) | 2.0+ |
| `union().distinct()` | [OK] Oui | Dédup manuel |

> [ATTENTION] **Piège d'examen :** En Spark, `union()` ne supprime **PAS** les doublons (contrairement au SQL UNION).
> Il se comporte comme SQL `UNION ALL`. Pour supprimer les doublons, appeler `.distinct()` après.

---

## Union de base

```scala
// union() — garde TOUTES les lignes y compris les doublons
val dfUnion = df1.union(df2)

// Union sans doublons
val dfDedup = df1.union(df2).distinct()
```

---

## Règles de correspondance des colonnes

`union()` fait correspondre les colonnes par **position**, PAS par nom.

```scala
import spark.implicits._

val df1 = Seq((1, "Alice")).toDF("id", "name")
val df2 = Seq(("Bob", 2)).toDF("name", "id")   // colonnes inversées

df1.union(df2).show()
// +---+-----+
// |id |name |
// +---+-----+
// |1  |Alice|
// |Bob|2    |   ← FAUX ! "Bob" dans la colonne id, 2 dans name
// +---+-----+
```

**Correction :** Réordonner les colonnes avant union.

```scala
df1.union(df2.select("id", "name")).show()   // Aligner par nom
```

---

## `unionByName()` — Correspondance par nom (Spark 3.1+)

```scala
// Fait correspondre les colonnes par nom, peu importe l'ordre
val dfUnion = df1.unionByName(df2)

// Autoriser les colonnes manquantes (remplies avec null)
val dfUnion = df1.unionByName(df2, allowMissingColumns = true)
```

---

## Union de plusieurs DataFrames

```scala
val dfs = Seq(df1, df2, df3, df4)

// Avec reduce
val dfAll = dfs.reduce(_ union _)

// Ou en enchaînant
val dfAll2 = df1.union(df2).union(df3).union(df4)
```

---

## Points clés pour l'examen

> [OK] `union()` = SQL `UNION ALL` → garde les doublons.

> [OK] Pour dédupliquer : `union().distinct()`.

> [OK] `unionByName()` est plus sûr que `union()` quand les colonnes peuvent être dans un ordre différent.

> [OK] En Scala, `reduce(_ union _)` remplace `reduce(DataFrame.union, dfs)` de Python.
