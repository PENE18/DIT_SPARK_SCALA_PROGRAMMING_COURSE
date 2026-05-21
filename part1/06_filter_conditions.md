# 06 — Conditions de filtre (Filter)

## `filter()` et `where()` — identiques

```scala
// Style SQL (string)
df.filter("age > 30")
df.where("salary > 5000 AND country = 'France'")

// Style Column (le plus courant)
df.filter(col("age") > 30)
df.filter(df("age") > 30)
```

---

## Opérateurs de comparaison

```scala
import org.apache.spark.sql.functions.col

df.filter(col("age") === 25)     // Égal        (=== en Scala, pas ==)
df.filter(col("age") =!= 25)    // Différent   (=!= en Scala, pas !=)
df.filter(col("age") > 25)      // Strictement supérieur
df.filter(col("age") >= 25)     // Supérieur ou égal
df.filter(col("age") < 25)      // Strictement inférieur
df.filter(col("age") <= 25)     // Inférieur ou égal
```

---

## Opérateurs logiques — AND, OR, NOT

```scala
// AND — utiliser && (ou &)
df.filter((col("age") > 25) && (col("salary") > 3000))

// OR — utiliser || (ou |)
df.filter((col("country") === "France") || (col("country") === "Germany"))

// NOT — utiliser ! (point d'exclamation)
df.filter(!(col("country") === "France"))

// Enchaînement de filtres (AND implicite)
df.filter(col("age") > 25)
  .filter(col("salary") > 3000)
  .filter(col("country") =!= "Unknown")
```

> [OK] **Différence Python → Scala :** En Python on utilise `&`, `|`, `~`. En Scala on utilise `&&`, `||`, `!`.
> Les priorités sont mieux gérées en Scala, mais garder les parenthèses reste une bonne pratique.

---

## `isin()` / `!isin()`

```scala
// Filtrer les lignes où country EST DANS une liste
df.filter(col("country").isin("France", "Germany", "Spain"))
df.filter(col("country").isin(List("France", "Germany", "Spain"): _*))

// Filtrer les lignes PAS dans la liste
df.filter(!col("country").isin("France", "Germany"))
```

---

## `isNull` / `isNotNull`

```scala
// Lignes où salary EST NULL
df.filter(col("salary").isNull)

// Lignes où salary N'EST PAS NULL
df.filter(col("salary").isNotNull)

// Combiné
df.filter(col("name").isNotNull && col("age").isNotNull)
```

---

## Conditions sur les chaînes

```scala
// LIKE (wildcards SQL: % et _)
df.filter(col("name").like("Jo%"))      // Commence par "Jo"
df.filter(col("name").like("%son"))     // Finit par "son"
df.filter(col("name").like("%ohn%"))    // Contient "ohn"

// RLIKE — expression régulière
df.filter(col("name").rlike("^Jo.*"))
df.filter(col("email").rlike(".*@gmail\\.com$"))

// startsWith / endsWith / contains
df.filter(col("name").startsWith("Jo"))
df.filter(col("name").endsWith("son"))
df.filter(col("name").contains("ohn"))
```

---

## `between()`

```scala
// Entre 25 et 40 inclus
df.filter(col("age").between(25, 40))

// Équivalent à :
df.filter((col("age") >= 25) && (col("age") <= 40))
```

---

## Filtre style SQL avec `spark.sql`

```scala
df.createOrReplaceTempView("employees")

val result = spark.sql("""
    SELECT *
    FROM employees
    WHERE age > 30
      AND salary BETWEEN 3000 AND 8000
      AND country IN ('France', 'Germany')
      AND name IS NOT NULL
""")
result.show()
```

---

## Exemple complexe

```scala
import org.apache.spark.sql.functions.{col, lower}

val result = df
  .filter(col("salary").isNotNull)
  .filter(col("age").between(25, 55))
  .filter(col("country").isin("France", "UK", "USA"))
  .filter(lower(col("name")).contains("martin"))
  .filter(!col("department").like("%HR%"))

result.show()
```

---

## Tableau des différences Python → Scala

| Python (PySpark) | Scala |
|-----------------|-------|
| `col("x") == "v"` | `col("x") === "v"` |
| `col("x") != "v"` | `col("x") =!= "v"` |
| `~col("x")` | `!col("x")` |
| `a & b` | `a && b` |
| `a \| b` | `a \|\| b` |
| `col("x").isNull()` | `col("x").isNull` (pas de parenthèses) |
