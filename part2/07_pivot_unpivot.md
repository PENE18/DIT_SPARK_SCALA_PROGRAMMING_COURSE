# 07 — Pivot & Unpivot

## Pivot — Lignes vers Colonnes

Le pivot transforme des valeurs distinctes d'une colonne en colonnes séparées (crosstab / tableau croisé dynamique).

### Pivot basique

```scala
val dfPivot = df.groupBy("nom").pivot("trimestre").sum("ventes")
dfPivot.show()
// +-----+----+----+----+
// |nom  |Q1  |Q2  |Q3  |
// +-----+----+----+----+
// |Alice|1000|1500|null|
// |Bob  |2000|2500|1800|
// +-----+----+----+----+
```

### Pivot avec valeurs explicites (plus rapide [OK])

```scala
// Sans liste : Spark doit scanner les données pour trouver les valeurs distinctes
// Avec liste  : évite ce scan supplémentaire → beaucoup plus rapide sur de gros datasets

val dfPivot = df.groupBy("nom")
  .pivot("trimestre", Seq("Q1", "Q2", "Q3", "Q4"))
  .sum("ventes")
```

### Pivot avec plusieurs agrégations

```scala
import org.apache.spark.sql.functions.{sum, avg}

val dfPivot = df.groupBy("nom")
  .pivot("trimestre")
  .agg(sum("ventes").alias("total"), avg("ventes").alias("moy"))
```

---

## Unpivot — Colonnes vers Lignes

### Avec `stack()` (Spark < 3.4)

```scala
import org.apache.spark.sql.functions.expr
import spark.implicits._

val dfWide = Seq(
  ("Alice", Some(1000), Some(1500), None),
  ("Bob",   Some(2000), Some(2500), Some(1800))
).toDF("nom", "Q1", "Q2", "Q3")

val dfUnpivot = dfWide.select(
  col("nom"),
  expr("stack(3, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3) as (trimestre, ventes)")
)
dfUnpivot.show()
```

### Filtrer les nulls après unpivot

```scala
val dfUnpivot = dfUnpivot.filter(col("ventes").isNotNull)
```

### Avec `unpivot()` natif — Spark 3.4+

```scala
val dfUnpivot = dfWide.unpivot(
  ids                 = Array("nom"),
  values              = Array("Q1", "Q2", "Q3"),
  variableColumnName  = "trimestre",
  valueColumnName     = "ventes"
)
```

---

## Points clés pour l'examen

> [ATTENTION] Sans liste de valeurs dans `pivot()`, Spark fait un scan supplémentaire. **Toujours fournir la liste en production.**

> [OK] `stack(N, 'k1', col1, 'k2', col2, ...)` — N = nombre de paires (clé, valeur).

> [OK] `unpivot()` natif disponible uniquement depuis Spark 3.4.
