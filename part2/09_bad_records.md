# 09 — Gestion des enregistrements malformés

## Les trois modes

| Mode | Comportement | Cas d'usage |
|------|-------------|-------------|
| `PERMISSIVE` | Stocke les lignes invalides dans `_corrupt_record` | Défaut, pour le débogage |
| `DROPMALFORMED` | Supprime silencieusement les lignes malformées | Pipelines tolérants |
| `FAILFAST` | Lance une exception à la première ligne invalide | Validation stricte |

## PERMISSIVE (défaut)

```scala
import org.apache.spark.sql.types._

val schemaWithCorrupt = StructType(Seq(
  StructField("id",              IntegerType, nullable = true),
  StructField("name",            StringType,  nullable = true),
  StructField("salary",          DoubleType,  nullable = true),
  StructField("_corrupt_record", StringType,  nullable = true)  // ← obligatoire dans le schéma
))

val df = spark.read
  .schema(schemaWithCorrupt)
  .option("header", "true")
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .csv("data.csv")

// Séparer les bons des mauvais
val good = df.filter(col("_corrupt_record").isNull).drop("_corrupt_record")
val bad  = df.filter(col("_corrupt_record").isNotNull)
```

## DROPMALFORMED

```scala
val df = spark.read
  .option("header", "true")
  .option("mode", "DROPMALFORMED")
  .csv("data.csv")
// Les lignes malformées sont supprimées silencieusement
```

## FAILFAST

```scala
try {
  val df = spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv("data.csv")
  df.show()   // L'exception est déclenchée ici (action)
} catch {
  case e: Exception =>
    println(s"Données invalides trouvées : ${e.getMessage}")
}
```

## Options utiles

```scala
val df = spark.read
  .option("nullValue", "NULL")    // Traiter "NULL" comme null
  .option("nullValue", "N/A")     // Traiter "N/A" comme null
  .option("emptyValue", "")       // Chaîne vide = null
  .option("nanValue", "NaN")      // Traiter "NaN" comme NaN
  .csv("data.csv")
```

## Résumé — Quel mode choisir ?

| Situation | Mode recommandé |
|-----------|-----------------|
| Développement / Diagnostic | `PERMISSIVE` + inspecter `_corrupt_record` |
| Production tolérante | `DROPMALFORMED` |
| Validation stricte des données | `FAILFAST` |

## Points clés pour l'examen

> [ATTENTION] La colonne `_corrupt_record` n'apparaît **que** si elle est incluse dans le schéma ET que l'option `columnNameOfCorruptRecord` est définie.

> [OK] En production : utiliser `PERMISSIVE` + écrire les lignes corrompues dans une table séparée pour audit.
