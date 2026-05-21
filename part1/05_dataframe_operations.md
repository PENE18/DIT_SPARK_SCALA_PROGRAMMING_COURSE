# 05 — Opérations sur les DataFrames

## Sélectionner des colonnes

```scala
import org.apache.spark.sql.functions.col

df.select("name", "age")
df.select(col("name"), col("age"))
df.select(df("name"), df("age"))
df.select(col("salary").alias("monthly_salary"))
df.select("*")
```

## Ajouter & Renommer des colonnes

```scala
import org.apache.spark.sql.functions.{col, lit, upper, round}

val df1 = df.withColumn("senior",       col("age") > 50)
val df2 = df.withColumn("tax",          col("salary") * 0.2)
val df3 = df.withColumn("name_upper",   upper(col("name")))
val df4 = df.withColumn("source",       lit("csv_import"))       // constante
val df5 = df.withColumn("salary",       col("salary").cast("double"))  // cast

val df6 = df.withColumnRenamed("name", "full_name")
val df7 = df.drop("colonne_inutile")
val df8 = df.drop("col1", "col2", "col3")
```

## Agrégations

```scala
import org.apache.spark.sql.functions.{count, sum, avg, min, max}

df.agg(count("*"), avg("salary"), max("age")).show()

df.groupBy("country")
  .agg(
    count("id").alias("nb_employes"),
    avg("salary").alias("salaire_moyen"),
    max("age").alias("age_max")
  ).show()
```

## Tri

```scala
import org.apache.spark.sql.functions.desc

df.orderBy("age")
df.orderBy(col("age").desc)
df.sort("country", "age")
df.orderBy(desc("salary"))
```

## Jointures

```scala
// Inner (défaut)
df1.join(df2, "id")
df1.join(df2, "id", "left")
df1.join(df2, "id", "right")
df1.join(df2, "id", "full")
df1.join(df2, "id", "leftsemi")   // semi → leftsemi en Scala
df1.join(df2, "id", "leftanti")   // anti → leftanti en Scala
```

## Points clés

> [ATTENTION] `withColumn()` ne modifie pas le DataFrame original — il retourne un **nouveau** DataFrame.
> Les DataFrames Spark sont **immuables**.

> [OK] Spark 3.3+ : `withColumns(Map("nom" -> expr, ...))` pour ajouter plusieurs colonnes en une fois.

> [ATTENTION] En Scala, l'égalité sur colonne s'écrit `===` (triple égal), pas `==` :
> `col("country") === "France"` — et la négation `=!=` ou `!(col("x") === "y")`.
