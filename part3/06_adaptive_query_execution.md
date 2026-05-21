# 06 — Adaptive Query Execution (AQE)

## Qu'est-ce que l'AQE ?

L'AQE (introduit dans Spark 3.0) ré-optimise les requêtes **à l'exécution** en se basant sur les statistiques réelles collectées pendant l'exécution — plutôt que sur les estimations faites lors de la planification.

```
Plan à la compilation (estimations)
          ↓
Exécuter Stage 1 → collecter les stats réelles (nombre de lignes, tailles)
          ↓
Ré-optimiser le plan restant avec des VRAIES stats
          ↓
Exécuter Stage 2 avec un meilleur plan
```

---

## Activer l'AQE

```scala
// Activer l'AQE (défaut: true dans Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

// Vérifier le réglage actuel
spark.conf.get("spark.sql.adaptive.enabled")
```

---

## Fonctionnalité 1 — Coalescence dynamique des partitions de shuffle

L'AQE fusionne automatiquement les petites partitions de shuffle pour éviter des milliers de micro-tâches.

```scala
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

// Avant AQE: 200 partitions de shuffle (défaut)
// Après AQE:  Spark fusionne les petites → moins de partitions, plus grandes
```

---

## Fonctionnalité 2 — Changement dynamique de stratégie de jointure

L'AQE peut basculer d'un Sort-Merge Join à un Broadcast Hash Join **à l'exécution** si un côté s'avère plus petit qu'attendu après filtrage.

```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")

// Exemple : une jointure planifiée comme Sort-Merge Join
// Mais après filtrage, un côté est réduit à 8 MB
// L'AQE détecte cela et bascule vers Broadcast Hash Join automatiquement
```

---

## Fonctionnalité 3 — Optimisation des jointures avec skew

Le skew de données = certaines partitions ont beaucoup plus de données que d'autres, créant des stragglers.

```scala
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Une partition est "skewed" si elle est plus grande que les deux seuils suivants :
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

L'AQE **divise automatiquement les partitions skewées** en sous-tâches et **duplique** la partition correspondante de l'autre côté.

---

## Toutes les configurations AQE

```scala
// Interrupteur principal
spark.conf.set("spark.sql.adaptive.enabled", "true")

// Coalescence des partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

// Jointure skew
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

---

## AQE vs Catalyst (comparaison)

| Fonctionnalité | Catalyst (Statique) | AQE (Dynamique) |
|----------------|---------------------|-----------------|\
| Timing | Avant l'exécution | Pendant l'exécution |
| Basé sur | Estimations | Stats réelles à l'exécution |
| Sélection de jointure | Au moment de la planification | Peut changer à l'exécution |
| Nombre de partitions | Fixe (200 par défaut) | Fusionné dynamiquement |
| Gestion du skew | Hints manuels | Automatique |

---

## Points clés pour l'examen

> [OK] Les **3 fonctionnalités principales de l'AQE** : (1) coalescence dynamique des partitions, (2) changement dynamique de stratégie de jointure, (3) optimisation du skew join.

> [OK] **AQE vs Catalyst** : Catalyst optimise statiquement à la planification. L'AQE ré-optimise à l'exécution avec les vraies stats.

> [OK] L'AQE est activé par défaut depuis **Spark 3.2**.
