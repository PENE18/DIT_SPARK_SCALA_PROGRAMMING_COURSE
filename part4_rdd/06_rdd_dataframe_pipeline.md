# 06 — RDD ↔ DataFrame & Pipeline Complet

## RDD → DataFrame

```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// Méthode 1 : toDF() avec case class (le plus idiomatique en Scala)
case class Employee(name: String, age: Int, salary: Double, country: String)

val rdd = sc.parallelize(Seq(
  ("Alice",  32, 5200.0, "France"),
  ("Bob",    45, 4800.0, "UK"),
  ("Carol",  28, 6100.0, "France")
))
val rddTyped = rdd.map { case (n, a, s, c) => Employee(n, a, s, c) }

import spark.implicits._
val df = rddTyped.toDF()

// Méthode 2 : createDataFrame() avec schéma explicite
val schema = StructType(Seq(
  StructField("name",    StringType,  nullable = true),
  StructField("age",     IntegerType, nullable = true),
  StructField("salary",  DoubleType,  nullable = true),
  StructField("country", StringType,  nullable = true)
))
val rowRdd = rdd.map { case (n, a, s, c) => Row(n, a, s, c) }
val df2    = spark.createDataFrame(rowRdd, schema)

// Méthode 3 : toDF() avec noms de colonnes
val df3 = rdd.toDF("name", "age", "salary", "country")
```

---

## DataFrame → RDD

```scala
// df.rdd → RDD[Row] (non typé)
val rowRdd: org.apache.spark.rdd.RDD[Row] = df.rdd
rowRdd.map(row => row.getString(0)).take(5)  // accès par index

// df.as[T].rdd → RDD[T] (typé avec case class)
case class Employee(name: String, age: Int, salary: Double, country: String)
val typedRdd: org.apache.spark.rdd.RDD[Employee] = df.as[Employee].rdd
typedRdd.map(e => e.name).take(5)           // accès par nom ✅
```

---

## Quand utiliser RDD vs DataFrame

```scala
// ✅ Utiliser DataFrame pour :
// — ETL standard, filtres, jointures, agrégations
// — Lire/écrire CSV, JSON, Parquet
// — Requêtes SQL
// — Intégration avec Spark ML

val result = spark.read.csv("data.csv")
  .filter(col("salary") > 4000)
  .groupBy("country")
  .agg(avg("salary"))

// ✅ Utiliser RDD pour :
// — Traitement non structuré (logs binaires, fichiers custom)
// — Algorithmes itératifs custom (graphes, ML maison)
// — Contrôle fin du partitionnement
// — Opérations impossibles en API DataFrame

val customResult = sc.textFile("logs/*.log")
  .filter(_.contains("ERROR"))
  .map(line => parseCustomFormat(line))
  .reduceByKey(_ + _)
```

---

## Pipeline complet RDD — Word Count (exemple classique)

```scala
val sc = spark.sparkContext

// 1. Lire
val lines = sc.textFile("data/text/corpus.txt")

// 2. Transformer
val words = lines
  .flatMap(_.toLowerCase.split("\\W+"))   // découper en mots
  .filter(_.nonEmpty)                      // supprimer les chaînes vides
  .filter(_.length > 2)                    // ignorer les mots courts

// 3. Compter
val wordCounts = words
  .map(word => (word, 1))
  .reduceByKey(_ + _)

// 4. Trier par fréquence décroissante
val sorted = wordCounts.sortBy(_._2, ascending = false)

// 5. Afficher le top 10
sorted.take(10).foreach { case (word, count) =>
  println(f"$word%-20s $count%5d")
}

// 6. Sauvegarder
sorted.saveAsTextFile("output/word_count/")
```

---

## Pipeline complet RDD — Analyse de logs

```scala
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class LogEntry(timestamp: String, level: String, message: String, ip: String)

def parseLogLine(line: String): Option[LogEntry] = {
  val pattern = """(\S+) (\S+) (.+) from (\S+)""".r
  line match {
    case pattern(ts, level, msg, ip) => Some(LogEntry(ts, level, msg, ip))
    case _                           => None
  }
}

val logs = sc.textFile("data/logs/*.log")

// Analyser et filtrer les lignes valides
val parsed = logs
  .flatMap(parseLogLine)
  .cache()   // utilisé plusieurs fois

// 1. Compter par niveau de log
val byLevel = parsed
  .map(e => (e.level, 1L))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)

println("=== Logs par niveau ===")
byLevel.collect().foreach { case (lvl, n) => println(s"$lvl : $n") }

// 2. IPs avec le plus d'erreurs
val errorIps = parsed
  .filter(_.level == "ERROR")
  .map(e => (e.ip, 1L))
  .reduceByKey(_ + _)
  .top(10)(Ordering.by(_._2))

println("\n=== Top 10 IPs avec erreurs ===")
errorIps.foreach { case (ip, n) => println(s"$ip : $n erreurs") }

// 3. Accumulateur pour métriques
val totalLines  = sc.longAccumulator("total_lines")
val parsedLines = sc.longAccumulator("parsed_lines")

logs.foreach { line =>
  totalLines.add(1)
  if (parseLogLine(line).isDefined) parsedLines.add(1)
}

println(s"\nTotal lignes  : ${totalLines.value}")
println(s"Lignes valides : ${parsedLines.value}")
println(f"Taux de parsing: ${parsedLines.value.toDouble / totalLines.value * 100}%.1f%%")

parsed.unpersist()
```

---

## Pipeline mixte RDD + DataFrame

```scala
// Lire un fichier custom non structuré via RDD
val rawRdd = sc.textFile("data/custom_format.txt")
  .filter(!_.startsWith("#"))   // ignorer les commentaires
  .map { line =>
    val parts = line.split("\\|")
    (parts(0).trim, parts(1).trim.toDouble, parts(2).trim)
  }

// Convertir en DataFrame pour exploiter Catalyst
import spark.implicits._
val df = rawRdd.toDF("name", "salary", "country")

// Continuer avec l'API DataFrame
df.filter(col("salary") > 4000)
  .groupBy("country")
  .agg(avg("salary").alias("avg_salary"), count("*").alias("count"))
  .orderBy(desc("avg_salary"))
  .show()
```

---

## Points clés pour l'examen

> ✅ `rdd.toDF()` avec `import spark.implicits._` est la conversion la plus simple (nécessite une case class ou un tuple).

> ✅ `df.rdd` retourne un `RDD[Row]` — accès aux champs par index. Utiliser `df.as[MyCaseClass].rdd` pour un accès typé.

> ✅ **Règle générale** : commencer par les DataFrames. N'utiliser les RDDs que si l'API DataFrame ne suffit pas.

> ⚠️ La conversion `df.rdd` perd les optimisations Catalyst — éviter si le DataFrame seul suffit.
