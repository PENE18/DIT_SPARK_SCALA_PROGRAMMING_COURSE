package part3_performance

/**
 * =============================================================
 *   Part 3 — Pipeline complet : Performance, Nulls, Cache,
 *             Repartition, Broadcast, AQE, Catalyst
 * =============================================================
 * Lancer depuis la racine du projet
 */

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object FullPipelinePart3 {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Part3_Performance")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "10mb")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n" + "=" * 60)
    println("  PART 3 — Performance & Optimisation")
    println("=" * 60)

    // ══════════════════════════════════════════════════════════
    // 1. REPARTITION vs COALESCE
    // ══════════════════════════════════════════════════════════
    println("\n── 1. Repartition vs Coalesce ──")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    println(s"Partitions initiales : ${df.rdd.getNumPartitions}")

    val dfRep = df.repartition(6)
    println(s"Après repartition(6)  : ${dfRep.rdd.getNumPartitions}")

    val dfCoal = dfRep.coalesce(2)
    println(s"Après coalesce(2)     : ${dfCoal.rdd.getNumPartitions}")

    val dfByCountry = df.repartition(4, col("country"))
    println(s"Repartition par 'country' (4): ${dfByCountry.rdd.getNumPartitions}")

    println("\n  Différences clés :")
    println("  repartition() → shuffle complet, peut augmenter ou diminuer")
    println("  coalesce()    → pas de shuffle, ne peut que diminuer")

    // ══════════════════════════════════════════════════════════
    // 2. CACHE / PERSIST
    // ══════════════════════════════════════════════════════════
    println("\n── 2. Cache & Persist ──")

    val dfClean = df.filter(col("salary").isNotNull).cache()
    val countCached = dfClean.count()
    println(s"DataFrame mis en cache ($countCached lignes)")

    val result1 = dfClean.groupBy("country").agg(avg("salary").alias("avg_salary"))
    val result2 = dfClean.groupBy("department").agg(count("*").alias("effectif"))

    println("Résultat 1 (depuis le cache) — salaire moyen par pays :")
    result1.orderBy("country").show()

    println("Résultat 2 (depuis le cache) — effectif par département :")
    result2.orderBy("department").show()

    val dfPersist = df.persist(StorageLevel.MEMORY_AND_DISK)
    dfPersist.count()
    println("DataFrame persisté avec MEMORY_AND_DISK")

    dfClean.unpersist()
    dfPersist.unpersist()
    println("Caches libérés (unpersist)")

    // ══════════════════════════════════════════════════════════
    // 3. CATALYST — EXPLAIN PLAN
    // ══════════════════════════════════════════════════════════
    println("\n── 3. Catalyst Optimizer — Plan d'exécution ──")

    val dfFiltered = df.filter(col("country") === "France").select("name", "salary", "country")

    println("Plan physique (explain) :")
    dfFiltered.explain()

    println("\nPlan formaté (explain formatted) :")
    dfFiltered.explain("formatted")

    println("\n→ Chercher 'PushedFilters' dans le plan — c'est le pushdown du filtre vers la source.")

    // ══════════════════════════════════════════════════════════
    // 4. BROADCAST JOIN
    // ══════════════════════════════════════════════════════════
    println("\n── 4. Broadcast Join ──")

    val dfDept = Seq(
      (1, "Engineering", "Tech"),
      (2, "Sales",       "Commercial"),
      (3, "HR",          "Support"),
      (4, "Marketing",   "Commercial")
    ).toDF("dept_id", "dept_name", "dept_category")

    val deptMap = Seq(
      ("Engineering", 1), ("Sales", 2), ("HR", 3)
    ).toDF("department", "dept_id")

    val dfWithId = df.join(deptMap, "department", "left")

    println("Broadcast join (force) :")
    val resultBroadcast = dfWithId.join(broadcast(dfDept), "dept_id", "left")
    resultBroadcast.select("name", "dept_name", "dept_category", "salary").show(5)

    println("Plan du broadcast join :")
    resultBroadcast.explain()

    // ══════════════════════════════════════════════════════════
    // 5. AQE — Adaptive Query Execution
    // ══════════════════════════════════════════════════════════
    println("\n── 5. Adaptive Query Execution (AQE) ──")

    println(s"AQE activé : ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(s"Coalescence partitions : ${spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")}")
    println(s"Seuil broadcast auto  : ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")

    val dfAgg = df.groupBy("country", "department").agg(avg("salary").alias("avg_sal"))
    println(s"\nPartitions avant groupBy : ${df.rdd.getNumPartitions}")
    println("Avec AQE, les petites partitions de shuffle seront automatiquement fusionnées.")
    dfAgg.show()

    // ══════════════════════════════════════════════════════════
    // 6. GESTION DES NULLS
    // ══════════════════════════════════════════════════════════
    println("\n── 6. Gestion des Nulls ──")

    val schema = StructType(Seq(
      StructField("id",      IntegerType),
      StructField("name",    StringType),
      StructField("age",     IntegerType),
      StructField("salary",  DoubleType),
      StructField("country", StringType)
    ))

    val rows = Seq(
      Row(1, "Alice",  32,   5200.0,    "France"),
      Row(2, "Bob",    null, 4800.0,    "UK"),
      Row(3, null,     28,   null,       "France"),
      Row(4, "Dave",   55,   7100.0,    null),
      Row(5, "Eve",    38,   Double.NaN, "France")
    )

    val dfNulls = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    println("DataFrame avec nulls :")
    dfNulls.show()

    println("Nombre de nulls par colonne :")
    dfNulls.select(dfNulls.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*).show()

    println("isNull (valeurs manquantes SQL) :")
    dfNulls.filter(col("salary").isNull).show()

    println("isnan (NaN — Not a Number) :")
    dfNulls.filter(isnan(col("salary"))).show()

    println("fillna — remplacer les nulls :")
    dfNulls.na.fill(Map[String, Any]("name" -> "Inconnu", "salary" -> 0.0, "country" -> "N/A", "age" -> -1)).show()

    println("dropna — supprimer les lignes avec des nulls :")
    dfNulls.na.drop(Seq("name", "salary")).show()

    println("coalesce — première valeur non-null :")
    dfNulls.withColumn("country_clean", coalesce(col("country"), lit("Pays inconnu"))).show()

    println("when/otherwise — logique conditionnelle sur nulls :")
    dfNulls.withColumn("statut_salaire",
      when(col("salary").isNull, "Salaire manquant")
      .when(isnan(col("salary")), "Salaire invalide (NaN)")
      .when(col("salary") < 0, "Salaire négatif")
      .otherwise("OK")
    ).show()

    println("orderBy avec contrôle des nulls :")
    dfNulls.orderBy(col("salary").asc_nulls_last).show()

    println("eqNullSafe — égalité sécurisée avec null :")
    dfNulls.filter(col("country").eqNullSafe("France")).show()
    dfNulls.filter(col("country").eqNullSafe(null)).show()

    println("concat_ws — concaténer en ignorant les nulls :")
    dfNulls.withColumn("info",
      concat_ws(" | ", col("name"), col("country"), upper(lit("emp")))
    ).show()

    // ══════════════════════════════════════════════════════════
    // 7. PIPELINE COMPLET
    // ══════════════════════════════════════════════════════════
    println("\n── 7. Pipeline complet optimisé ──")

    val dfBase = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .cache()

    dfBase.count()  // Déclencher le cache

    val dfResult = dfBase
      .filter(col("salary").isNotNull)
      .filter(col("age").between(28, 60))
      .withColumn("salary_band",
        when(col("salary") > 6000, "Senior")
        .when(col("salary") > 4500, "Mid")
        .otherwise("Junior"))
      .groupBy("country", "salary_band")
      .agg(
        count("*").alias("effectif"),
        avg("salary").alias("salaire_moyen")
      )
      .orderBy("country", "salary_band")

    println("Résultat final :")
    dfResult.show()

    dfBase.unpersist()

    // ══════════════════════════════════════════════════════════
    // Fin
    // ══════════════════════════════════════════════════════════
    spark.stop()
    println("\n🛑 SparkSession arrêtée. Pipeline Part 3 terminé !\n")
  }
}
