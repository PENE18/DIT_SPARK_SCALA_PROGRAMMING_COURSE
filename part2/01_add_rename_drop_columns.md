# 01 — Ajouter, Renommer & Supprimer des colonnes

## `withColumn()` — Ajouter ou remplacer

```scala
import org.apache.spark.sql.functions.{col, lit, upper, round}

val df1 = df.withColumn("source",         lit("import_csv"))         // constante
val df2 = df.withColumn("salaire_annuel", col("salaire_mensuel") * 12)
val df3 = df.withColumn("est_senior",     col("age") >= 60)
val df4 = df.withColumn("nom_majuscule",  upper(col("nom")))
val df5 = df.withColumn("salaire_arrondi",round(col("salaire"), 2))
val df6 = df.withColumn("salaire",        col("salaire").cast("double"))  // remplacement
```

> [ATTENTION] `withColumn()` ne modifie **pas** le DataFrame original — il retourne un **nouveau** DataFrame.

## `withColumns()` — Plusieurs colonnes (Spark 3.3+)

```scala
// En Scala, on enchaîne les withColumn() ou on utilise withColumns (Map)
val dfMulti = df
  .withColumn("taxe",   col("salaire") * 0.2)
  .withColumn("net",    col("salaire") * 0.8)
  .withColumn("source", lit("csv"))

// Spark 3.3+ — API withColumns avec Map
val dfMulti2 = df.withColumns(Map(
  "taxe"   -> col("salaire") * 0.2,
  "net"    -> col("salaire") * 0.8,
  "source" -> lit("csv")
))
```

## `withColumnRenamed()` — Renommer

```scala
val df1 = df.withColumnRenamed("nom", "nom_complet")
val df2 = df.withColumnRenamed("ddn", "date_naissance")
```

## Renommer via `select()` + `alias()`

```scala
val dfRenamed = df.select(
  col("emp_id").alias("id"),
  col("emp_nom").alias("nom"),
  col("salaire")
)
```

## `drop()` — Supprimer

```scala
val df1 = df.drop("col_inutile")
val df2 = df.drop("col1", "col2", "col3")

val colsASupprimer = Seq("col1", "col2")
val df3 = df.drop(colsASupprimer: _*)
```

> [ATTENTION] `drop()` ne lève **pas** d'erreur si la colonne n'existe pas — il ne fait rien silencieusement.

## Cast de type

```scala
val df1 = df.withColumn("age",     col("age").cast("integer"))
val df2 = df.withColumn("salaire", col("salaire").cast("double"))
val df3 = df.withColumn("embauche",col("embauche").cast("date"))
```

## Tableau récapitulatif

| Opération | Méthode | Notes |
|-----------|---------|-------|
| Ajouter | `withColumn(nom, expr)` | Remplace si le nom existe |
| Ajouter plusieurs | `withColumns(Map(...))` | Spark 3.3+ |
| Renommer | `withColumnRenamed(ancien, nouveau)` | Un à la fois |
| Renommer (select) | `col("x").alias("y")` | Plusieurs à la fois |
| Supprimer | `drop("col1", "col2")` | Silencieux si absent |
| Changer de type | `col("x").cast("type")` | Utiliser avec withColumn |
