# 05 — Accumulateurs & Variables Broadcast

## Accumulateurs

Les **accumulateurs** sont des variables partagées en **écriture seule** depuis les executors.
Le driver peut lire leur valeur finale après une action.

```
Driver          Executor A       Executor B
  acc = 0  →   acc += 3    |    acc += 5
               acc += 2    |    acc += 1
  ← acc = 11  ─────────────────────────
```

### Accumulateurs numériques (built-in)

```scala
// LongAccumulator
val countAcc = sc.longAccumulator("lignes_traitees")

// DoubleAccumulator
val sumAcc = sc.doubleAccumulator("somme_salaires")

// Utiliser dans un RDD
val rdd = sc.parallelize(Seq(
  ("Alice",   5000.0, "France"),
  ("Bob",     4000.0, "UK"),
  ("Carol",   null.asInstanceOf[Double], "France"),
  ("Dave",    6000.0, "Germany")
))

rdd.foreach { case (name, salary, country) =>
  countAcc.add(1L)
  if (salary != null && !salary.isNaN) sumAcc.add(salary)
}

println(s"Lignes traitées : ${countAcc.value}")   // → 4
println(s"Somme salaires  : ${sumAcc.value}")     // → 15000.0
```

### Accumulateur personnalisé

```scala
import org.apache.spark.util.AccumulatorV2

// Accumulateur pour collecter des erreurs
class ErrorAccumulator extends AccumulatorV2[String, List[String]] {
  private var _errors: List[String] = List.empty

  def isZero: Boolean = _errors.isEmpty
  def copy(): ErrorAccumulator = {
    val acc = new ErrorAccumulator
    acc._errors = this._errors
    acc
  }
  def reset(): Unit = _errors = List.empty
  def add(v: String): Unit = _errors = _errors :+ v
  def merge(other: AccumulatorV2[String, List[String]]): Unit =
    _errors = _errors ++ other.value
  def value: List[String] = _errors
}

val errorAcc = new ErrorAccumulator
sc.register(errorAcc, "erreurs")

rdd.foreach { case (name, salary, _) =>
  if (salary == null || salary.isNaN)
    errorAcc.add(s"Salaire manquant pour $name")
}

println("Erreurs collectées :")
errorAcc.value.foreach(println)
```

### Pièges des accumulateurs

```scala
// ⚠️ Piège 1 : les accumulateurs dans les transformations lazy peuvent être
// comptés plusieurs fois si le RDD est réévalué (ex: sans cache)
val acc = sc.longAccumulator("count")

val rddFiltered = rdd.filter { x =>
  acc.add(1)   // ⚠️ compté à chaque évaluation du RDD !
  x > 0
}

rddFiltered.count()   // 1er calcul → acc = N
rddFiltered.collect() // 2ème calcul → acc = 2N  ← double comptage !

// ✅ Solution : mettre en cache le RDD intermédiaire
val rddCached = rdd.filter { x => acc.add(1); x > 0 }.cache()
rddCached.count()     // acc = N
rddCached.collect()   // lu depuis le cache → acc inchangé ✅

// ⚠️ Piège 2 : les accumulateurs sont en écriture seule depuis les executors
// Le driver NE PEUT PAS lire leur valeur à l'intérieur d'une transformation
val badAcc = sc.longAccumulator("bad")
rdd.map { x =>
  badAcc.add(1)
  println(badAcc.value)  // ⚠️ imprimé sur l'executor, non fiable !
  x
}.count()
```

---

## Variables Broadcast

Les **variables broadcast** distribuent une valeur en **lecture seule** à tous les executors,
envoyée **une seule fois** via un protocole BitTorrent-like (pas avec chaque tâche).

```scala
// Créer une variable broadcast
val lookup = Map(
  "FR" -> "France",
  "UK" -> "Royaume-Uni",
  "DE" -> "Allemagne",
  "US" -> "États-Unis"
)
val bcLookup = sc.broadcast(lookup)

// Utiliser dans un RDD (accès via .value)
val rdd = sc.parallelize(Seq(("Alice", "FR"), ("Bob", "DE"), ("Carol", "UK")))

val expanded = rdd.map { case (name, code) =>
  val country = bcLookup.value.getOrElse(code, s"Inconnu ($code)")
  (name, country)
}

expanded.collect().foreach { case (n, c) => println(s"$n → $c") }
// Alice → France
// Bob   → Allemagne
// Carol → Royaume-Uni
```

### Broadcast d'un objet plus complexe

```scala
// Broadcaster une liste de codes à exclure
val blacklist = Set("XX", "YY", "ZZ")
val bcBlacklist = sc.broadcast(blacklist)

val filtered = rdd.filter { case (_, code) =>
  !bcBlacklist.value.contains(code)
}

// Libérer la mémoire executor (ne plus utiliser après ça)
bcBlacklist.unpersist()   // supprime des executors, garde dans le driver
bcBlacklist.destroy()     // supprime partout (driver + executors)
```

### Broadcast vs envoi par tâche

```scala
// ❌ Sans broadcast — le dictionnaire est sérialisé avec chaque tâche
val lookup = Map("FR" -> "France", /* ... 1 million d'entrées ... */)
rdd.map { case (name, code) =>
  lookup.getOrElse(code, "?")  // closure capturée → sérialisée N fois !
}

// ✅ Avec broadcast — envoyé UNE FOIS par executor
val bcLookup = sc.broadcast(lookup)
rdd.map { case (name, code) =>
  bcLookup.value.getOrElse(code, "?")
}
```

---

## Tableau comparatif

| Caractéristique | Accumulateur | Variable Broadcast |
|-----------------|-------------|-------------------|
| Direction | Executors → Driver | Driver → Executors |
| Accès | Écriture seule (executors) | Lecture seule (executors) |
| Type | Numérique ou custom | Tout objet sérialisable |
| Cas d'usage | Compteurs, métriques, logs | Lookup tables, config, référentiels |
| Valeur finale | Lue par le driver après action | Lue par les tâches pendant l'exécution |

---

## Points clés pour l'examen

> ⚠️ Les accumulateurs dans des **transformations lazy** peuvent être **comptés plusieurs fois** si le RDD est réévalué. Utiliser `cache()` pour éviter ça.

> ✅ Les accumulateurs sont **write-only** pour les executors — leur valeur dans le driver n'est fiable qu'**après une action**.

> ✅ Les variables broadcast évitent de sérialiser une grande structure de données avec **chaque tâche**. Elles sont envoyées **une fois par executor**.

> ✅ `bcVar.destroy()` supprime la variable broadcast **partout** (driver + executors).
