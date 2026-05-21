# 01 — Spark Structured Streaming

## Qu'est-ce que le Streaming ?

Le Structured Streaming traite des **flux de données en temps réel** en continu.
Il traite un flux live comme une table non bornée qui grandit en permanence.

```
Source (Kafka / Fichiers / Socket)
        ↓
  Table non bornée (nouvelles lignes = nouvelles données)
        ↓
  Requête continue (tourne indéfiniment)
        ↓
  Sink (Console / Fichier / Kafka / Mémoire)
```

## Deux APIs

| API | Statut | Notes |
|-----|--------|-------|
| DStream (legacy) | Ancien — basé sur RDD | Spark 1.x, à éviter |
| **Structured Streaming** | [OK] Actuel | API DataFrame/SQL, sémantique exactly-once |

## Concepts clés

| Concept | Description |
|---------|-------------|
| **Source** | Origine des données streaming (Kafka, fichiers, socket) |
| **Trigger** | Fréquence de traitement des nouvelles données |
| **Output Mode** | Comment les résultats sont écrits (append, update, complete) |
| **Sink** | Destination des résultats |
| **Checkpoint** | Stocke l'état pour la tolérance aux pannes |
| **Watermark** | Seuil pour gérer les données arrivant en retard |

## Structure d'une requête streaming

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

val spark = SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()

// 1. Lire le stream (Source)
val dfStream = spark.readStream
  .format("csv")
  .option("header", "true")
  .schema(schema)
  .load("/mnt/input/")

// 2. Transformer
val result = dfStream
  .filter(col("amount") > 1000)
  .groupBy("category")
  .sum("amount")

// 3. Écrire le stream (Sink)
val query = result.writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "/mnt/checkpoint/")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
  .start()

// 4. Attendre la fin
query.awaitTermination()
```

## Sources

```scala
// Source fichier (surveille un dossier)
val df = spark.readStream.format("csv").option("header", "true").schema(schema).load("/mnt/incoming/")

// Source Kafka
val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "mon_topic")
  .option("startingOffsets", "latest")
  .load()

// Parser la valeur Kafka (bytes → string → JSON)
import org.apache.spark.sql.functions.from_json
val dfParsed = df.select(
  col("key").cast("string"),
  from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

// Source socket (tests uniquement)
val df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

// Source rate (génère des données à débit fixe — tests)
val df = spark.readStream.format("rate").option("rowsPerSecond", "100").load()
```

## Modes de sortie

| Mode | Quand l'utiliser | Agrégations supportées |
|------|-----------------|------------------------|
| `append` | Nouvelles lignes seulement | Filtres simples, pas d'agrégation |
| `update` | Seulement les lignes modifiées | Agrégations avec groupBy |
| `complete` | Table de résultats complète | Toujours avec agrégations |

## Triggers

```scala
import org.apache.spark.sql.streaming.Trigger

.trigger(Trigger.ProcessingTime("10 seconds"))   // Intervalle fixe
.trigger(Trigger.Once())                          // Batch unique (traite tout, puis s'arrête)
.trigger(Trigger.Continuous("1 second"))          // Continu (faible latence, expérimental)
// Par défaut : aussi vite que possible (pas de trigger = micro-batch rapide)
```

## Sinks

```scala
// Console (débogage)
df.writeStream.format("console").start()

// Fichier (parquet / csv / json)
df.writeStream.format("parquet")
  .option("path", "/mnt/output/")
  .option("checkpointLocation", "/mnt/checkpoint/")
  .start()

// Kafka
df.writeStream.format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("topic", "output_topic")
  .option("checkpointLocation", "/mnt/checkpoint/")
  .start()

// ForeachBatch (logique personnalisée par micro-batch)
df.writeStream.foreachBatch { (batchDf: org.apache.spark.sql.DataFrame, batchId: Long) =>
  batchDf.write.mode("append").saveAsTable("delta_table")
}.start()
```

## Watermarks — Données en retard

```scala
import org.apache.spark.sql.functions.window

val dfWithWm = dfStream
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window(col("event_time"), "5 minutes"),
    col("user_id")
  )
  .count()
```

## Types de fenêtres

```scala
import org.apache.spark.sql.functions.{window, session_window}

// Fenêtre tumbling (non chevauchante)
window(col("ts"), "5 minutes")

// Fenêtre glissante (chevauchante)
window(col("ts"), "10 minutes", "5 minutes")   // 10 min de fenêtre, glisse toutes les 5 min

// Fenêtre session (Spark 3.2+)
session_window(col("ts"), "10 minutes")         // Inactivité de 10 min = nouvelle session
```

## Streaming vs Batch

| Fonctionnalité | Batch | Structured Streaming |
|----------------|-------|---------------------|
| Point d'entrée | `spark.read` | `spark.readStream` |
| Sortie | `df.write` | `df.writeStream` |
| Exécution | Finie, tourne une fois | Continue, infinie |
| Tolérance aux pannes | Relancer le job | Checkpointing |

## Points clés pour l'examen

> [OK] **Batch** = `spark.read` / `df.write` — **Streaming** = `spark.readStream` / `df.writeStream`

> [OK] Les **3 output modes** : `append` (nouvelles lignes), `update` (lignes modifiées), `complete` (table entière).

> [OK] Le **checkpoint** est obligatoire pour la tolérance aux pannes en streaming.

> [OK] Le **watermark** définit combien de temps Spark attend les données en retard avant de fermer une fenêtre.
