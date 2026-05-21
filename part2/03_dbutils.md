# 03 — DBUtils (Databricks)

> [ATTENTION] DBUtils est **spécifique à Databricks** — non disponible dans un environnement Spark standalone.
> L'API DBUtils est principalement exposée en **Python et Scala** dans les notebooks Databricks.

## Accéder à DBUtils en Scala

```scala
// Dans les notebooks Databricks — disponible automatiquement
// dbutils est un objet global injecté par Databricks

// Dans les jobs Databricks (import explicite)
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
```

---

## Utilitaires Système de Fichiers — `dbutils.fs`

```scala
// Lister les fichiers / dossiers
dbutils.fs.ls("/mnt/data/")
dbutils.fs.ls("dbfs:/FileStore/")

// Créer un dossier
dbutils.fs.mkdirs("/mnt/data/nouveau_dossier/")

// Copier un fichier
dbutils.fs.cp("/mnt/data/fichier.csv", "/mnt/backup/fichier.csv")

// Déplacer un fichier
dbutils.fs.mv("/mnt/data/ancien.csv", "/mnt/data/nouveau.csv")

// Supprimer (recurse=true pour les dossiers)
dbutils.fs.rm("/mnt/data/fichier.csv")
dbutils.fs.rm("/mnt/data/dossier/", recurse = true)

// Lire un petit fichier texte
val contenu = dbutils.fs.head("/mnt/data/fichier.txt", maxBytes = 65536)

// Monter un stockage externe (Azure Blob / S3)
dbutils.fs.mount(
  source      = "wasbs://container@compte.blob.core.windows.net/",
  mountPoint  = "/mnt/mesdonnes",
  extraConfigs = Map("fs.azure.account.key.compte.blob.core.windows.net" -> "<clé>")
)

// Lister les points de montage
dbutils.fs.mounts()

// Démonter
dbutils.fs.unmount("/mnt/mesdonnes")
```

---

## Widgets — `dbutils.widgets`

```scala
// Créer des widgets
dbutils.widgets.text("nom", "valeurDefaut", "Étiquette")
dbutils.widgets.dropdown("env", "dev", Seq("dev", "staging", "prod"), "Environnement")
dbutils.widgets.combobox("fruit", "pomme", Seq("pomme", "banane", "cerise"))
dbutils.widgets.multiselect("cols", "id", Seq("id", "nom", "salaire"))

// Lire la valeur d'un widget
val env = dbutils.widgets.get("env")
println(s"Exécution en : $env")

// Supprimer les widgets
dbutils.widgets.remove("nom")
dbutils.widgets.removeAll()
```

---

## Secrets — `dbutils.secrets`

```scala
// Lister les scopes de secrets
dbutils.secrets.listScopes()

// Lister les clés d'un scope
dbutils.secrets.list("mon-scope")

// Lire un secret (valeur masquée dans la sortie)
val password = dbutils.secrets.get(scope = "mon-scope", key = "db-password")
```

---

## Utilitaires Notebook — `dbutils.notebook`

```scala
// Exécuter un autre notebook et attendre le résultat
val result = dbutils.notebook.run(
  "/chemin/vers/notebook",
  timeoutSeconds = 300,
  arguments = Map("env" -> "prod", "table" -> "ventes")
)

// Quitter un notebook avec une valeur de retour
dbutils.notebook.exit("SUCCÈS")
dbutils.notebook.exit("{'statut': 'ok', 'count': 1234}")
```

---

## Tableau récapitulatif

| Utilitaire | Commande | Rôle |
|------------|----------|------|
| fs | `dbutils.fs.ls(chemin)` | Lister le répertoire |
| fs | `dbutils.fs.cp(src, dst)` | Copier un fichier |
| fs | `dbutils.fs.rm(chemin, recurse)` | Supprimer |
| fs | `dbutils.fs.mount(source, point)` | Monter le stockage |
| widgets | `dbutils.widgets.text(nom, defaut)` | Créer un input texte |
| widgets | `dbutils.widgets.get(nom)` | Lire la valeur |
| secrets | `dbutils.secrets.get(scope, clé)` | Lire un secret |
| notebook | `dbutils.notebook.run(chemin, timeout)` | Exécuter un notebook enfant |
| notebook | `dbutils.notebook.exit(valeur)` | Quitter avec valeur de retour |
