# 04 — Catalyst Optimizer

## Qu'est-ce que le Catalyst ?

Le Catalyst Optimizer est le **moteur d'optimisation des requêtes** de Spark SQL.
Il transforme votre code DataFrame/SQL en le plan d'exécution physique le plus efficace.

```
Code utilisateur (API DataFrame / SQL)
          ↓
    Plan logique non résolu
          ↓ [Analyse]
    Plan logique résolu
          ↓ [Optimisation logique]
    Plan logique optimisé
          ↓ [Planification physique]
    Plans physiques (plusieurs candidats)
          ↓ [Modèle de coût]
    Plan physique sélectionné
          ↓ [Génération de code — Tungsten]
    Bytecode JVM
```

## Les 4 phases

### Phase 1 — Analyse
- Résout les noms de colonnes et les références de tables
- Valide le schéma par rapport au catalogue
- Remplace les attributs non résolus par des attributs résolus

### Phase 2 — Optimisation logique

| Optimisation | Description |
|--------------|-------------|
| **Predicate Pushdown** | Déplace les `filter()` le plus près possible de la source (lire moins de données) |
| **Column Pruning** | Lit uniquement les colonnes réellement nécessaires |
| **Constant Folding** | Évalue les expressions constantes à la planification |
| **Boolean Simplification** | Simplifie les expressions booléennes complexes |
| **Null Propagation** | Simplifie les expressions impliquant des nulls |

### Phase 3 — Planification physique
- Convertit le plan logique en plans physiques
- Sélectionne le meilleur plan selon le coût (statistiques)
- Décide de la stratégie de jointure : broadcast hash join, sort-merge join, etc.

### Phase 4 — Génération de code (Tungsten)
- Génère du bytecode JVM optimisé
- Whole-stage code generation
- Beaucoup plus rapide que l'exécution interprétée

## Inspecter le plan d'exécution

```scala
df.explain()                  // Plan physique seulement
df.explain("extended")        // Les 4 plans
df.explain("cost")            // Avec estimations de coût
df.explain("formatted")       // Bien formaté (Spark 3.0+)
```

## Lire le plan

```
== Physical Plan ==
*(1) Project [name#10, salary#11]
+- *(1) Filter (isnotnull(country#12) AND (country#12 = France))
   +- *(1) ColumnarToRow
      +- FileScan parquet [name#10,salary#11,country#12]
         PushedFilters: [IsNotNull(country), EqualTo(country,France)]
```

- `PushedFilters` → le predicate pushdown fonctionne [OK]
- `(1)` → whole-stage code generation actif [OK]
- `BroadcastHashJoin` → petite table broadcastée [OK]
- `SortMergeJoin` → jointure large-large (shuffle potentiel) [ATTENTION]

## Exemples d'optimisations automatiques

```scala
import org.apache.spark.sql.functions.{col, lit}

// Ces deux requêtes produisent le MÊME plan
// (Catalyst réordonne automatiquement)
df.filter(col("age") > 30).select("name", "age")
df.select("name", "age").filter(col("age") > 30)

// Constant folding : 100*10 est évalué à la planification
df.filter(col("valeur") > lit(100) * lit(10))  // → filter(col > 1000)
```

## Points clés pour l'examen

> [OK] Les **4 phases Catalyst** : Analyse → Optimisation Logique → Planification Physique → Génération de Code.

> [OK] **Predicate Pushdown** = déplacer les filtres vers la source pour lire moins de données.

> [OK] **Tungsten** = moteur d'exécution générant du bytecode JVM optimisé (whole-stage code gen).

> [OK] Vérifier le plan avec `df.explain("formatted")`.
