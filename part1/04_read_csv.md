# 04 — Lire des fichiers CSV

## Lecture basique

```scala
// Forme la plus simple
val df = spark.read.csv("data.csv")

// Avec en-tête
val df = spark.read.option("header", "true").csv("data.csv")

// Inférer les types automatiquement
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data.csv")
```

---

## Toutes les options CSV

```scala
val df = spark.read
  .option("header", "true")                           // 1ère ligne = noms de colonnes
  .option("inferSchema", "true")                      // Détection auto des types
  .option("sep", ",")                                 // Délimiteur (défaut: virgule)
  .option("quote", "\"")                              // Caractère de citation
  .option("escape", "\\")                             // Caractère d'échappement
  .option("nullValue", "NA")                          // Traiter "NA" comme null
  .option("nanValue", "NaN")                          // Traiter "NaN" comme NaN
  .option("dateFormat", "yyyy-MM-dd")                 // Format de date
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")  // Format timestamp
  .option("multiLine", "true")                        // Champs multi-lignes
  .option("encoding", "UTF-8")                        // Encodage du fichier
  .option("mode", "DROPMALFORMED")                    // Gestion des erreurs
  .csv("data.csv")
```

---

## Modes de gestion des erreurs

| Mode | Comportement |
|------|-------------|
| `PERMISSIVE` | Défaut. Les données malformées sont mises dans `_corrupt_record` |
| `DROPMALFORMED` | Supprime silencieusement les lignes malformées |
| `FAILFAST` | Lance une exception à la première ligne malformée |

---

## Définir un schéma explicite [OK] (Bonne pratique en production)

Évite `inferSchema=true` — nécessite un scan supplémentaire du fichier.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("id",         IntegerType, nullable = false),
  StructField("name",       StringType,  nullable = true),
  StructField("age",        IntegerType, nullable = true),
  StructField("salary",     DoubleType,  nullable = true),
  StructField("country",    StringType,  nullable = true),
  StructField("department", StringType,  nullable = true),
  StructField("hire_date",  StringType,  nullable = true)
))

val df = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("data/csv/employees.csv")

df.printSchema()
df.show(5)
```

---

## Lire plusieurs fichiers CSV

```scala
// Lire tous les CSV d'un dossier
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/folder/")

// Lire des fichiers spécifiques
val df = spark.read.option("header", "true").csv("file1.csv", "file2.csv")

// Pattern wildcard
val df = spark.read.option("header", "true").csv("data/2024-*.csv")
```

---

## Inspecter un DataFrame

```scala
df.printSchema()          // Affiche les types des colonnes
df.show(10)               // Affiche les 10 premières lignes
df.show(truncate = false) // Affiche les valeurs complètes
df.count()                // Nombre de lignes
df.columns                // Array des noms de colonnes
df.dtypes                 // Array de tuples (nom, type)
df.describe().show()      // Statistiques descriptives
df.head(3)                // Retourne les 3 premières lignes comme Array[Row]
```

---

## Points clés pour l'examen

> [ATTENTION] `inferSchema=true` déclenche un scan complet du fichier — **éviter en production**.

> [ATTENTION] `nullable = false` dans le schéma est **metadata uniquement** — Spark ne l'applique pas à l'exécution.

> [OK] Toujours définir le schéma manuellement pour des pipelines robustes.
