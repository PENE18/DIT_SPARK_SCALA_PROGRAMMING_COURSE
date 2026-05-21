package part3_performance

/**
 * =============================================================
 *   02 — Cache & Persist : niveaux de stockage, unpersist
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object CachePersistDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("Cache_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── Sans cache : recalcul à chaque action
    println("\n── Sans cache ──")
    val t0 = System.currentTimeMillis()
    df.filter(col("salary").isNotNull).groupBy("country").agg(avg("salary")).collect()
    df.filter(col("salary").isNotNull).groupBy("department").agg(count("*")).collect()
    println(f"  Temps sans cache : ${(System.currentTimeMillis() - t0) / 1000.0}%.3fs (2 passes sur les données)")

    // ── Avec cache()
    println("\n── Avec cache() ──")
    val dfClean = df.filter(col("salary").isNotNull).cache()

    val t1 = System.currentTimeMillis()
    val cnt = dfClean.count()  // déclenche le cache
    println(s"  $cnt lignes mises en cache")
    val t2 = System.currentTimeMillis()

    // Les appels suivants utilisent le cache
    dfClean.groupBy("country").agg(avg("salary")).collect()
    dfClean.groupBy("department").agg(count("*")).collect()
    println(f"  Temps avec cache : ${(System.currentTimeMillis() - t2) / 1000.0}%.3fs (lecture depuis le cache)")
    println(s"  Niveau de stockage : ${dfClean.storageLevel}")

    // Libérer
    dfClean.unpersist()
    println("  Cache libéré (unpersist)")

    // ── persist() avec niveaux explicites
    println("\n── persist() avec niveaux ──")

    val dfMem  = df.persist(StorageLevel.MEMORY_ONLY)
    val dfDisk = df.persist(StorageLevel.DISK_ONLY)
    val dfBoth = df.persist(StorageLevel.MEMORY_AND_DISK)

    dfMem.count()
    dfDisk.count()
    dfBoth.count()

    println(s"  MEMORY_ONLY      : ${dfMem.storageLevel}")
    println(s"  DISK_ONLY        : ${dfDisk.storageLevel}")
    println(s"  MEMORY_AND_DISK  : ${dfBoth.storageLevel}")

    dfMem.unpersist()
    dfDisk.unpersist()
    dfBoth.unpersist()

    // ── Cacher une vue SQL
    println("\n── cacheTable / uncacheTable ──")
    df.createOrReplaceTempView("employees")
    spark.catalog.cacheTable("employees")
    println(s"  Table 'employees' en cache : ${spark.catalog.isCached("employees")}")
    spark.catalog.uncacheTable("employees")
    println(s"  Après uncache : ${spark.catalog.isCached("employees")}")

    println(
      """
        |Règle d'or :
        |  ✅ Cacher si le DataFrame est utilisé 2+ fois dans le même job
        |  ✅ Cacher pour les boucles itératives (ML, algorithmes)
        |  ❌ Ne pas cacher si utilisé une seule fois
        |""".stripMargin)

    spark.stop()
  }
}
