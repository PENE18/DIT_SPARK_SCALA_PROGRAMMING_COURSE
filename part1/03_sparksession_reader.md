# 03 — SparkSession & Spark Reader

## SparkSession

`SparkSession` est le **point d'entrée unique** vers Spark depuis Spark 2.0.
Il unifie `SparkContext`, `SQLContext` et `HiveContext`.

```scala
import org.apache.spark.sql.SparkSession

// Créer une SparkSession
val spark = SparkSession.builder()
  .appName("ExamPrep")
  .master("local[*]")
  .getOrCreate()

// Accéder au contexte sous-jacent
val sc = spark.sparkContext

// Arrêter la session
spark.stop()
```

---

## DataFrameReader (`spark.read`)

`spark.read` retourne un objet `DataFrameReader` — l'interface standard pour charger des données.

```scala
spark.read.<format>(<path>)
```

## Formats supportés

| Format | Méthode | Exemple |
|--------|---------|---------|\
| CSV | `.csv()` | `spark.read.csv("data.csv")` |
| JSON | `.json()` | `spark.read.json("data.json")` |
| Parquet | `.parquet()` | `spark.read.parquet("data.parquet")` |
| ORC | `.orc()` | `spark.read.orc("data.orc")` |
| Text | `.text()` | `spark.read.text("data.txt")` |
| JDBC | `.jdbc()` | `spark.read.jdbc(url, table, props)` |
| Delta | `.format("delta").load()` | Tables Delta Lake |

## API générique `.format()`

```scala
val df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("path/to/file.csv")
```

---

## Points clés pour l'examen

- `spark.read` est **lazy** — aucune donnée n'est lue avant qu'une action soit déclenchée.
- Préférer `.schema(schema)` à `inferSchema=true` en production (évite un scan supplémentaire).
- `getOrCreate()` retourne une session existante s'il y en a déjà une.
