# Spark Exam Prep — Scala Edition

Conversion complète de tous les fichiers Python/PySpark en Spark Scala.

## Structure

```
src/
├── part1_basics/
│   ├── 01_SparkSessionDemo.scala       # SparkSession, formats, SparkContext
│   ├── 02_ReadCsvDemo.scala            # Lire CSV : schéma, modes d'erreur
│   ├── 03_DataFrameOpsDemo.scala       # select, withColumn, groupBy, join
│   ├── 04_FilterDemo.scala             # filter, isin, between, like, SQL
│   └── 05_FullPipelinePart1.scala      # Pipeline complet Part 1
├── part2_transforms/
│   ├── 01_ColumnsDemo.scala            # withColumn, cast, rename, drop
│   ├── 02_JoinsDemo.scala              # inner/left/right/full/semi/anti
│   ├── 03_ExplodeDemo.scala            # explode, explode_outer, posexplode, Map
│   ├── 04_WhenOtherwiseDemo.scala      # when/otherwise (CASE WHEN)
│   ├── 05_UnionDemo.scala              # union, unionByName, allowMissingColumns
│   ├── 06_PivotUnpivotDemo.scala       # pivot, stack() unpivot
│   ├── 07_JsonFlattenDemo.scala        # JSON imbriqué, flatten récursif, from_json
│   ├── 08_BadRecordsDemo.scala         # PERMISSIVE / DROPMALFORMED / FAILFAST
│   └── 09_FullPipelinePart2.scala      # Pipeline complet Part 2
└── part3_performance/
    ├── 01_RepartitionCoalesceDemo.scala # repartition vs coalesce
    ├── 02_CachePersistDemo.scala        # cache(), persist(), StorageLevel
    ├── 03_CatalystExplainDemo.scala     # explain, pushdown, pruning, folding
    ├── 04_BroadcastDemo.scala           # broadcast join, broadcast variable + UDF
    ├── 05_AqeDemo.scala                 # AQE : coalesce, dynamic join, skew
    ├── 06_NullsDemo.scala               # isNull, isnan, fillna, dropna, coalesce
    └── 07_FullPipelinePart3.scala       # Pipeline complet Part 3
```

## Différences Python → Scala à retenir

| PySpark                          | Spark Scala                                      |
|----------------------------------|--------------------------------------------------|
| `df.filter(col("x") > 5)`       | `df.filter(col("x") > 5)` (identique)           |
| `col("a") == "val"`              | `col("a") === "val"` (triple égal)               |
| `~col("x")`                      | `!col("x")` ou `col("x").isNull`                 |
| `df.filter(a & b)`               | `df.filter(a && b)`                              |
| `df.filter(a \| b)`              | `df.filter(a \|\| b)`                            |
| `from pyspark.sql.functions import` | `import org.apache.spark.sql.functions._`     |
| `spark_sum()`                    | `sum()` (ou `functions.sum`)                     |
| `StorageLevel.MEMORY_ONLY`       | `StorageLevel.MEMORY_ONLY`                       |
| `leftsemi` / `leftanti`          | `"leftsemi"` / `"leftanti"`                      |
| `reduce(DataFrame.union, dfs)`   | `dfs.reduce(_ union _)`                          |
| `df.na.fill(dict)`               | `df.na.fill(Map[String, Any](...))`              |
| `float("nan")`                   | `Double.NaN`                                     |
| `udf(fn, StringType())`          | `udf((x: String) => ...) ` (typage implicite)    |

## Prérequis

- Scala 2.12.x
- Apache Spark 3.5.x
- sbt 1.x (ou spark-submit)

## Lancement

```bash
# Compiler
sbt compile

# Lancer un fichier (exemple)
sbt "runMain SparkSessionDemo"

# Ou via spark-submit après assembly
spark-submit --class SparkSessionDemo target/scala-2.12/spark-exam-prep-scala_2.12-1.0.jar
```

## Variable d'environnement

Définir `PROJECT_ROOT` pour pointer vers la racine du projet contenant `data/` :
```bash
export PROJECT_ROOT=/chemin/vers/spark-exam-prep
```
