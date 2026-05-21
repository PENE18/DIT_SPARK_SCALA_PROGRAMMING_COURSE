# 04 — Fonction Explode

`explode()` convertit une colonne array ou map en **plusieurs lignes** — une ligne par élément.

## Tableau récapitulatif

| Fonction | Garde vide/null ? | Position ? |
|----------|-------------------|------------|
| `explode()` | [NON] Non | [NON] Non |
| `explode_outer()` | [OK] Oui (null) | [NON] Non |
| `posexplode()` | [NON] Non | [OK] Oui |
| `posexplode_outer()` | [OK] Oui (null) | [OK] Oui |

## `explode()` — Supprime les tableaux vides/null

```scala
import org.apache.spark.sql.functions.{explode, col}

// (1, Alice, [Python, Spark, SQL]) → 3 lignes
// (3, Carol, [])                   → SUPPRIMÉE

df.withColumn("skill", explode(col("skills"))).show()
```

## `explode_outer()` — Garde les lignes vides

```scala
import org.apache.spark.sql.functions.explode_outer

// (3, Carol, []) → (3, Carol, null)   ← ligne gardée avec null

df.withColumn("skill", explode_outer(col("skills"))).show()
```

## `posexplode()` — Ajoute l'index de position

```scala
import org.apache.spark.sql.functions.posexplode

// Retourne deux colonnes : pos et skill
df.select(col("id"), col("name"), posexplode(col("skills")).as(Seq("pos", "skill"))).show()
// pos=0 → Python, pos=1 → Spark, pos=2 → SQL
```

## Explode sur une colonne Map (clé-valeur)

```scala
import org.apache.spark.sql.functions.explode

// {"Python": 5, "Spark": 4} → 2 lignes : (Python, 5) et (Spark, 4)
dfMap.select(
  col("id"),
  col("name"),
  explode(col("scores")).as(Seq("competence", "score"))
).show()
```

## Points clés pour l'examen

> [ATTENTION] `explode()` sur un tableau vide `[]` → la **ligne est supprimée**. Utiliser `explode_outer()` pour la garder.

> [OK] Après `explode()`, l'ancienne colonne array est remplacée par une colonne scalaire.

> [OK] En Scala, `.alias("a", "b")` pour nommer deux colonnes à la fois s'écrit `.as(Seq("a", "b"))`.
