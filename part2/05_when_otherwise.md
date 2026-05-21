# 05 — when / otherwise (CASE WHEN)

Équivalent Scala du SQL `CASE WHEN ... THEN ... ELSE ... END`.

## Syntaxe de base

```scala
import org.apache.spark.sql.functions.{when, col}

val df = df.withColumn("categorie",
  when(col("age") < 18, "Mineur")
  .otherwise("Adulte")
)
```

## Conditions multiples (if / elif / else)

```scala
val df = df.withColumn("note",
  when(col("score") >= 90, "A")
  .when(col("score") >= 80, "B")
  .when(col("score") >= 70, "C")
  .when(col("score") >= 60, "D")
  .otherwise("F")
)
```

## Avec des nulls

```scala
import org.apache.spark.sql.functions.{coalesce, lit}

val df = df.withColumn("salaire_clean",
  when(col("salaire").isNull, 0.0)
  .otherwise(col("salaire"))
)
// Équivalent avec coalesce :
val df = df.withColumn("salaire_clean", coalesce(col("salaire"), lit(0.0)))
```

## Conditions complexes (AND / OR)

```scala
val df = df.withColumn("bonus",
  when((col("dept") === "Sales") && (col("salaire") > 5000), col("salaire") * 0.2)
  .when((col("dept") === "IT") || (col("annees") > 5), col("salaire") * 0.15)
  .otherwise(col("salaire") * 0.05)
)
```

## Utilisation dans `select()`

```scala
df.select(
  col("nom"),
  col("salaire"),
  when(col("salaire") > 5000, "Haut")
  .when(col("salaire") > 3000, "Moyen")
  .otherwise("Bas")
  .alias("tranche_salaire")
).show()
```

## SQL équivalent

```sql
SELECT nom, salaire,
  CASE
    WHEN salaire > 5000 THEN 'Haut'
    WHEN salaire > 3000 THEN 'Moyen'
    ELSE 'Bas'
  END AS tranche_salaire
FROM employes
```

## Points clés pour l'examen

> [ATTENTION] Si `otherwise()` est **omis**, les lignes qui ne correspondent à aucune condition `when()` reçoivent `null` — pas d'erreur.

> [OK] Les conditions dans `when()` utilisent `&&`, `||`, `!` (opérateurs Scala).

> [OK] `when()` peut être utilisé dans `withColumn()`, `select()` et même `filter()`.
