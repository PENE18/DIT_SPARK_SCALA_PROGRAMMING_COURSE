# 02 — Déploiement de Cluster

## Modes de déploiement

### Local Mode
- Tout tourne sur une machine (driver + executor dans la même JVM)
- Utilisé pour le développement et les tests

```scala
val spark = SparkSession.builder()
  .master("local")      // 1 thread
  .master("local[4]")   // 4 threads
  .master("local[*]")   // tous les cœurs disponibles
  .appName("MyApp")
  .getOrCreate()
```

### Standalone Mode
- Gestionnaire de cluster intégré de Spark
- Simple à configurer

```bash
# Démarrer le master
$SPARK_HOME/sbin/start-master.sh

# Démarrer un worker
$SPARK_HOME/sbin/start-worker.sh spark://master-host:7077

# Soumettre un job
spark-submit \
  --master spark://master-host:7077 \
  --deploy-mode cluster \
  --class com.example.MyApp \
  my_app.jar
```

### YARN Mode (Hadoop)
- Le plus courant en environnement d'entreprise

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --class com.example.MyApp \
  my_app.jar
```

### Kubernetes Mode
- Déploiements conteneurisés

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-spark-image:latest \
  --class com.example.MyApp \
  my_app.jar
```

---

## Client vs Cluster Mode

| Fonctionnalité | Client Mode | Cluster Mode |
|----------------|-------------|--------------|\
| Emplacement du Driver | Machine de soumission | Nœud worker |
| Idéal pour | Interactif / debug | Jobs en production |
| Logs | Disponibles localement | À récupérer depuis le cluster |
| Réseau | Driver → Executors (risque si distant) | Tout sur le même réseau |

---

## Options spark-submit importantes

```bash
spark-submit \
  --master <url>                   # URL du gestionnaire de cluster
  --deploy-mode <client|cluster>   # Où tourne le driver
  --name "MyApp"                   # Nom de l'application
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.dynamicAllocation.enabled=true \
  --jars dep1.jar,dep2.jar \       # JARs supplémentaires
  --files config.json \            # Fichiers pour tous les nœuds
  --class com.example.MyApp \
  my_app.jar
```

---

## Configurations importantes

```scala
val spark = SparkSession.builder()
  .appName("MyApp")
  .config("spark.executor.memory", "4g")
  .config("spark.executor.cores", "2")
  .config("spark.sql.shuffle.partitions", "200")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.dynamicAllocation.enabled", "true")
  .getOrCreate()
```

---

## Points clés pour l'examen

> [ATTENTION] **Piège :** En **cluster mode**, les logs du driver ne sont pas disponibles localement — il faut les récupérer depuis le cluster.

> [ATTENTION] **Piège :** `local[*]` utilise tous les cœurs de la machine locale, mais ne simule pas un vrai cluster distribué.
