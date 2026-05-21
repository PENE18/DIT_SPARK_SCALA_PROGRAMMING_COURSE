# 08 — Lire & Aplatir du JSON

## Lire un fichier JSON

```scala
// Lecture simple
val df = spark.read.json("data.json")

// Avec options
val df = spark.read
  .option("multiLine", "true")     // JSON indenté sur plusieurs lignes
  .option("mode", "PERMISSIVE")
  .json("data.json")

// Plusieurs fichiers
val df = spark.read.json("file1.json", "file2.json")
val df = spark.read.json("data/folder/*.json")
```

## Exemple de JSON imbriqué

```json
{
  "id": 1,
  "name": "Alice",
  "address": { "city": "Paris", "country": "France" },
  "skills": ["Python", "Spark"],
  "scores": { "python": 95, "spark": 90 }
}
```

## Schéma inféré automatiquement

```
root
 |-- id: long
 |-- name: string
 |-- address: struct
 |    |-- city: string
 |    |-- country: string
 |-- skills: array
 |    |-- element: string
 |-- scores: struct
 |    |-- python: long
 |    |-- spark: long
```

## Accéder aux champs imbriqués

```scala
// Notation pointée (dot notation)
df.select("address.city", "address.country").show()

// Avec col()
df.select(col("address.city"), col("scores.python")).show()
```

## Aplatir un struct (niveau 1)

```scala
val dfFlat = df.select(
  col("id"),
  col("name"),
  col("address.city").alias("city"),
  col("address.country").alias("country"),
  col("scores.python").alias("python_score"),
  col("scores.spark").alias("spark_score")
)
```

## Aplatir récursivement (profond)

```scala
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

def flattenDf(df: DataFrame): DataFrame = {
  var result = df
  var hasStruct = result.schema.fields.exists(_.dataType.isInstanceOf[StructType])
  while (hasStruct) {
    val structCols = result.schema.fields.filter(_.dataType.isInstanceOf[StructType])
    val colName    = structCols.head.name
    val nestedCols = result.select(s"$colName.*").columns
      .map(sub => col(s"$colName.$sub").alias(s"${colName}_$sub"))
    val otherCols  = result.columns.filter(_ != colName).map(col)
    result    = result.select(otherCols ++ nestedCols: _*)
    hasStruct = result.schema.fields.exists(_.dataType.isInstanceOf[StructType])
  }
  result
}

val dfFlat = flattenDf(df)
```

## Aplatir tableau + struct ensemble

```scala
import org.apache.spark.sql.functions.explode_outer

// Étape 1 : Explode la colonne tableau
val dfExploded = df.withColumn("skill", explode_outer(col("skills")))

// Étape 2 : Aplatir les structs
val dfFlat = dfExploded.select(
  col("id"), col("name"), col("skill"),
  col("address.city").alias("city"),
  col("scores.python").alias("python_score")
)
```

## Parser une colonne JSON string

```scala
import org.apache.spark.sql.functions.{from_json, col}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

val jsonSchema = StructType(Seq(
  StructField("city",    StringType),
  StructField("country", StringType)
))

val df2 = df.withColumn("address_parsed", from_json(col("address_json_string"), jsonSchema))
val df3 = df2.withColumn("city", col("address_parsed.city"))
```

## Points clés pour l'examen

> [OK] Utiliser `option("multiLine", "true")` pour les fichiers JSON indentés (pretty-printed).

> [OK] La notation `col("struct.field")` fonctionne pour accéder aux champs imbriqués.

> [OK] `from_json()` est utilisé quand le JSON est stocké **comme une chaîne** dans une colonne.
