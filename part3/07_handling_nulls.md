# 07 — Gestion des Nulls

## Comportement des nulls dans Spark

- Toute opération arithmétique avec `null` → `null`
- Toute comparaison avec `null` → `null` (ni vrai ni faux)
- `null == null` → `null` (pas `true`) → utiliser `eqNullSafe()`
- `count(*)` inclut les nulls ; `count(col)` les **exclut**

## Détecter les nulls

```scala
import org.apache.spark.sql.functions.{col, isnan, count, when}

// Filtrer les nulls
df.filter(col("salaire").isNull)
df.filter(col("salaire").isNotNull)

// isnan() — NaN flottant (pas un null SQL)
df.filter(isnan(col("salaire")))

// Les deux à la fois
df.filter(col("salaire").isNull || isnan(col("salaire")))

// Compter les nulls par colonne
df.select(
  df.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*
).show()
```

## `fillna()` / `na.fill()`

```scala
df.na.fill(0)                              // numériques → 0
df.na.fill("Inconnu")                      // chaînes → "Inconnu"
df.na.fill(0, Seq("salaire"))              // colonne spécifique
df.na.fill(Map[String, Any](
  "nom"     -> "Inconnu",
  "salaire" -> 0.0,
  "pays"    -> "N/A"
))
```

## `dropna()` / `na.drop()`

```scala
df.na.drop()                               // toute null → supprimer
df.na.drop("all")                          // toutes nulles → supprimer
df.na.drop(Seq("nom", "salaire"))          // nulls sur colonnes spécifiques
df.na.drop(minNonNulls = 3)               // garder si au moins 3 non-null
```

## `na.replace()`

```scala
df.na.replace("pays", Map("N/A" -> null.asInstanceOf[String]))
df.na.replace("pays", Map("N/A" -> "Inconnu", "NULL" -> "Inconnu"))
df.na.replace(Seq("salaire"), Map(0 -> -1))
```

## `coalesce()` — Première valeur non-null

```scala
import org.apache.spark.sql.functions.{coalesce, col, lit}

df.withColumn("email_final",
  coalesce(col("email_principal"), col("email_secondaire"), lit("defaut@email.com"))
)
```

## `when/otherwise` pour les nulls

```scala
import org.apache.spark.sql.functions.{when, isnan}

df.withColumn("statut",
  when(col("salaire").isNull, "Manquant")
  .when(isnan(col("salaire")), "Invalide")
  .when(col("salaire") < 0, "Négatif")
  .otherwise("OK")
)
```

## `eqNullSafe()` — Égalité null-safe

```scala
// Égalité normale : null === null → null (pas true !)
df.filter(col("nom") === null)             // [NON] Retourne 0 lignes

// Égalité null-safe : null <=> null → true
df.filter(col("nom").eqNullSafe(null))     // [OK] Correct
df.filter(col("nom").eqNullSafe("Alice"))  // [OK] Aussi valide
```

## Nulls dans `orderBy()`

```scala
df.orderBy(col("salaire").asc)              // nulls en DERNIER par défaut (asc)
df.orderBy(col("salaire").desc)             // nulls en PREMIER par défaut (desc)
df.orderBy(col("salaire").asc_nulls_first)  // forcer nulls en premier
df.orderBy(col("salaire").asc_nulls_last)   // forcer nulls en dernier
```

## `concat_ws()` — Ignore les nulls

```scala
import org.apache.spark.sql.functions.{concat, concat_ws}

// concat() → null si UN argument est null
df.withColumn("full_name", concat(col("prenom"), lit(" "), col("nom")))

// concat_ws() → ignore les nulls (plus sûr)
df.withColumn("full_name", concat_ws(" ", col("prenom"), col("nom")))
```

## Tableau de référence rapide

| Fonction | Entrée | Comportement |
|----------|--------|-------------|
| `isNull` | Colonne | True si null |
| `isNotNull` | Colonne | True si non-null |
| `isnan()` | Numérique | True si NaN |
| `na.fill(val)` | DF | Remplace null par val |
| `na.drop()` | DF | Supprime lignes avec nulls |
| `na.replace(ancien, nouveau)` | DF | Remplace valeur |
| `coalesce(c1, c2, ...)` | Colonnes | Premier non-null |
| `eqNullSafe(val)` | Colonne | Égalité null-safe |
| `concat_ws(sep, ...)` | Colonnes | Concatène, ignore nulls |

## Différences Python → Scala à noter

| Python | Scala |
|--------|-------|
| `col("x").isNull()` | `col("x").isNull` (pas de `()`) |
| `col("x").isNotNull()` | `col("x").isNotNull` (pas de `()`) |
| `df.fillna({"a": 0, "b": "X"})` | `df.na.fill(Map[String, Any]("a" -> 0, "b" -> "X"))` |
| `df.dropna(subset=["a","b"])` | `df.na.drop(Seq("a", "b"))` |
| `asc_nulls_last("col")` (fonction) | `col("x").asc_nulls_last` (méthode) |
