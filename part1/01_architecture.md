# 01 — Architecture Spark

## Composants clés

| Composant | Rôle |
|-----------|------|
| **Driver** | Orchestre le job, contient le SparkContext, construit le plan d'exécution |
| **Cluster Manager** | Alloue les ressources (YARN, Mesos, Kubernetes, Standalone) |
| **Executor** | Exécute les tâches sur les nœuds workers, stocke les données en mémoire/disque |
| **Task** | Plus petite unité de travail envoyée à un executor |
| **Job** | Déclenché par une action (ex: `collect()`, `show()`) |
| **Stage** | Ensemble de tâches délimitées par des shuffles |
| **DAG** | Directed Acyclic Graph — graphe de toutes les transformations |

## Flux d'exécution

```
User Code (Driver)
    ↓
SparkContext → DAG Scheduler → Task Scheduler
                                    ↓
                          Cluster Manager (YARN / K8s / Standalone)
                                    ↓
                         Executors (Worker Nodes)
                          [Task] [Task] [Task]
```

## Hiérarchie d'exécution

```
Application
   Job  (déclenché par une action)
         Stage  (délimité par un shuffle)
               Task  (1 partition = 1 task)
```

## Points clés pour l'examen

- Le **Driver** ne traite pas de données — il coordonne.
- Un **shuffle** est coûteux : il transfère des données entre executors via le réseau.
- Le **DAG Scheduler** divise le plan en stages. Le **Task Scheduler** envoie les tasks aux executors.
- `show()`, `count()`, `collect()` → actions qui **déclenchent** l'exécution.
- `filter()`, `select()`, `groupBy()` → transformations **lazy** qui ne font rien seules.
