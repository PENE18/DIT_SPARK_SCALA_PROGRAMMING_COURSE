# 03 — Cache vs Persist

## Pourquoi mettre en cache ?

Par défaut, Spark **recalcule un DataFrame depuis le début à chaque action**.
Si vous utilisez le même DataFrame plusieurs fois, c'est du gaspillage.

```
Sans cache:
df → filter → groupBy → show()     ← calcul complet
df → filter → groupBy → count()    ← calcul complet ENCORE (gâchis!)

Avec cache:
df → filter → groupBy → cache()    ← calcul + stockage
                      → show()     ← lecture depuis le cache
                      → count()    ← lecture depuis le cache (rapide!)
```

---

## `cache()`

`cache()` est un raccourci pour `persist()` avec le niveau de stockage par défaut : **MEMORY_AND_DISK**.

```scala
// Mettre en cache le DataFrame
df.cache()

// cache() est lazy — calcul et cache déclenchés à la première action
df.cache()
df.count()   // ← déclenche le calcul et le cache

// Libérer la mémoire
df.unpersist()
```

---

## `persist(storageLevel)`

`persist()` permet de choisir le niveau de stockage explicitement.

```scala
import org.apache.spark.storage.StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.MEMORY_ONLY_2)      // 2 répliques
df.persist(StorageLevel.OFF_HEAP)           // Mémoire hors tas
```

---

## Niveaux de stockage

| Niveau | Mémoire | Disque | Sérialisé | Répliques | Cas d'usage |
|--------|---------|--------|-----------|-----------|-------------|
| `MEMORY_ONLY` | [OK] | [NON] | [NON] | 1 | Rapide, tient en RAM |
| `MEMORY_AND_DISK` | [OK] | [OK] (débordement) | [NON] | 1 | **Défaut de cache()** |
| `MEMORY_ONLY_SER` | [OK] | [NON] | [OK] | 1 | Moins de RAM, plus de CPU |
| `MEMORY_AND_DISK_SER` | [OK] | [OK] | [OK] | 1 | Équilibre RAM/CPU |
| `DISK_ONLY` | [NON] | [OK] | [OK] | 1 | Peu de RAM disponible |
| `MEMORY_ONLY_2` | [OK] | [NON] | [NON] | 2 | Tolérance aux pannes |
| `OFF_HEAP` | Hors tas | [NON] | [OK] | 1 | Éviter la pression GC |

---

## Quand mettre en cache — règles

```scala
import org.apache.spark.sql.functions.{col, count, sum, avg}

// [OK] BON — DataFrame utilisé plusieurs fois
val dfClean = df.filter(col("status") === "active").cache()
val result1 = dfClean.groupBy("region").count()
val result2 = dfClean.groupBy("dept").sum("salary")
val result3 = dfClean.join(dfRef, "id")

// [NON] MAUVAIS — utilisé une seule fois (overhead inutile)
df.filter(col("x") > 5).cache().show()   // inutile

// [OK] BON — algorithmes itératifs (ex: boucles ML)
dfFeatures.cache()
for (_ <- 1 to 10) {
  val model = train(dfFeatures)
}
```

---

## Cacher une vue SQL

```scala
df.createOrReplaceTempView("employees")
spark.catalog.cacheTable("employees")

// Dés-cacher
spark.catalog.uncacheTable("employees")

// Effacer tous les caches
spark.catalog.clearCache()
```

---

## Points clés pour l'examen

> [ATTENTION] Le niveau de stockage par défaut de `cache()` est **MEMORY_AND_DISK** (pas MEMORY_ONLY).

> [ATTENTION] `cache()` est **lazy** — le cache ne se construit qu'à la première action après `cache()`.

> [OK] Pour libérer la mémoire : `df.unpersist()`.

> [OK] Utiliser `DISK_ONLY` quand le dataset est trop grand pour la mémoire mais qu'on veut éviter le recalcul.
