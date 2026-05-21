# 05 — Variables Broadcast & Broadcast Join

## Qu'est-ce qu'une variable broadcast ?

Une variable broadcast est une **variable en lecture seule mise en cache sur chaque executor**,
envoyée une seule fois plutôt qu'avec chaque tâche.

```
SANS broadcast :
  Tâche 1 → Driver envoie table_lookup → Executor A
  Tâche 2 → Driver envoie table_lookup → Executor A  (encore !)
  Tâche 3 → Driver envoie table_lookup → Executor B
  ...

AVEC broadcast :
  Driver envoie table_lookup → Executor A  (une fois, mis en cache)
  Driver envoie table_lookup → Executor B  (une fois, mis en cache)
  Toutes les tâches sur A/B utilisent la copie locale
```

## Broadcast Join automatique

Spark bascule automatiquement vers un broadcast join si une table est sous le seuil.

```scala
// Vérifier/modifier le seuil (défaut : 10 MB)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")       // → "10485760"
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

// Désactiver le broadcast auto
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

## Broadcast Join manuel — `broadcast()` hint

```scala
import org.apache.spark.sql.functions.broadcast

// Forcer le broadcast de la petite table
val result = largeDf.join(broadcast(smallDf), "id")

// Hint SQL
spark.sql("""
    SELECT /*+ BROADCAST(dept) */ e.*, d.dept_name
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
""")
```

## Variable broadcast (pour les UDF)

```scala
import org.apache.spark.sql.functions.udf

// Créer un dictionnaire lookup
val lookup = Map("US" -> "États-Unis", "FR" -> "France", "DE" -> "Allemagne")

// Le broadcaster
val bcLookup = spark.sparkContext.broadcast(lookup)

// Utiliser dans une UDF
val expandCountryUdf = udf((code: String) =>
  bcLookup.value.getOrElse(code, "Inconnu")
)

val df = df.withColumn("pays_complet", expandCountryUdf(col("code_pays")))

// Libérer quand terminé
bcLookup.unpersist()
bcLookup.destroy()
```

## Comparaison des stratégies de jointure

| Stratégie | Quand | Notes |
|-----------|-------|-------|
| **Broadcast Hash Join** | Petite table < seuil | Le plus rapide, pas de shuffle |
| **Shuffle Hash Join** | Tables moyennes | Shuffle, construction de hash map |
| **Sort-Merge Join** | Grandes tables (défaut) | Shuffle + tri des deux côtés |
| **Cartesian Join** | Cross join | Dangereux — N×M lignes |

## Points clés pour l'examen

> [OK] Spark utilise automatiquement un broadcast join si une table < `spark.sql.autoBroadcastJoinThreshold` (10 MB par défaut).

> [OK] **`broadcast()` (fonction SQL)** vs **`sparkContext.broadcast()`** :
> - `broadcast()` → pour les jointures DataFrame
> - `sparkContext.broadcast()` → pour partager des variables (Maps, listes) avec les UDFs

> [OK] Le broadcast join **élimine le shuffle** — c'est son principal avantage de performance.

> [OK] En Scala, les UDFs s'écrivent directement avec des lambdas typés : `udf((x: String) => ...)`.
