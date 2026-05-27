package part4_rdd

/**
 * =============================================================
 *   Part 4 — 05 : Accumulateurs & Variables Broadcast
 *   LongAccumulator, DoubleAccumulator, AccumulatorV2 custom,
 *   broadcast, pièges
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

// ── Accumulateur personnalisé pour collecter des erreurs
class ErrorAccumulator extends AccumulatorV2[String, List[String]] {
  private var _errors: List[String] = List.empty

  def isZero: Boolean     = _errors.isEmpty
  def copy(): ErrorAccumulator = { val a = new ErrorAccumulator; a._errors = _errors; a }
  def reset(): Unit        = _errors = List.empty
  def add(v: String): Unit = _errors = _errors :+ v
  def merge(other: AccumulatorV2[String, List[String]]): Unit =
    _errors = _errors ++ other.value
  def value: List[String]  = _errors
}

// ── Accumulateur pour statistiques (min, max, sum, count)
class StatsAccumulator extends AccumulatorV2[Double, Map[String, Double]] {
  private var _min:   Double = Double.MaxValue
  private var _max:   Double = Double.MinValue
  private var _sum:   Double = 0.0
  private var _count: Long   = 0L

  def isZero: Boolean = _count == 0L
  def copy(): StatsAccumulator = {
    val a = new StatsAccumulator
    a._min = _min; a._max = _max; a._sum = _sum; a._count = _count
    a
  }
  def reset(): Unit = { _min = Double.MaxValue; _max = Double.MinValue; _sum = 0.0; _count = 0L }
  def add(v: Double): Unit = {
    _min   = math.min(_min, v)
    _max   = math.max(_max, v)
    _sum  += v
    _count += 1
  }
  def merge(other: AccumulatorV2[Double, Map[String, Double]]): Unit = {
    val o = other.value
    _min   = math.min(_min,   o("min"))
    _max   = math.max(_max,   o("max"))
    _sum  += o("sum")
    _count = _count + o("count").toLong
  }
  def value: Map[String, Double] = Map(
    "min"   -> _min,
    "max"   -> _max,
    "sum"   -> _sum,
    "count" -> _count.toDouble,
    "avg"   -> (if (_count > 0) _sum / _count else 0.0)
  )
}

object AccumulatorsBroadcastDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Accumulators_Broadcast_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

    // ══════════════════════════════════════════════
    // 1. Accumulateurs numériques built-in
    // ══════════════════════════════════════════════
    println("\n── 1. LongAccumulator & DoubleAccumulator ──")

    val countAcc   = sc.longAccumulator("lignes_traitees")
    val sumAcc     = sc.doubleAccumulator("somme_salaires")
    val nullAcc    = sc.longAccumulator("valeurs_nulles")

    case class Record(name: String, salary: Option[Double], country: String)

    val records = sc.parallelize(Seq(
      Record("Alice",  Some(5200.0), "France"),
      Record("Bob",    None,          "UK"),
      Record("Carol",  Some(6100.0), "France"),
      Record("Dave",   Some(4800.0), "Germany"),
      Record("Eve",    None,          "UK"),
      Record("Frank",  Some(5500.0), "Germany")
    ))

    records.foreach { r =>
      countAcc.add(1L)
      r.salary match {
        case Some(s) => sumAcc.add(s)
        case None    => nullAcc.add(1L)
      }
    }

    println(s"Lignes traitées : ${countAcc.value}")
    println(s"Somme salaires  : ${sumAcc.value}")
    println(s"Valeurs nulles  : ${nullAcc.value}")
    println(f"Moyenne salaires (sans nulls) : ${sumAcc.value / (countAcc.value - nullAcc.value)}%.2f")

    // ══════════════════════════════════════════════
    // 2. Accumulateur personnalisé — ErrorAccumulator
    // ══════════════════════════════════════════════
    println("\n── 2. Accumulateur personnalisé (erreurs) ──")

    val errorAcc = new ErrorAccumulator
    sc.register(errorAcc, "erreurs_pipeline")

    val rawData = sc.parallelize(Seq(
      "Alice,5200,France",
      "Bob,INVALID,UK",       // ← donnée malformée
      "Carol,6100,France",
      ",4800,Germany",        // ← nom manquant
      "Eve,5000,UK"
    ))

    val cleaned = rawData.flatMap { line =>
      val parts = line.split(",")
      if (parts.length != 3) {
        errorAcc.add(s"Mauvais format : '$line'")
        None
      } else {
        val (name, salaryStr, country) = (parts(0), parts(1), parts(2))
        if (name.isEmpty) {
          errorAcc.add(s"Nom manquant : '$line'")
          None
        } else {
          try   { Some((name, salaryStr.toDouble, country)) }
          catch { case _: NumberFormatException =>
            errorAcc.add(s"Salaire invalide '$salaryStr' pour '$name'")
            None
          }
        }
      }
    }

    println(s"Lignes valides : ${cleaned.count()}")
    println("Erreurs collectées :")
    errorAcc.value.foreach(e => println(s"  ⚠️  $e"))

    // ══════════════════════════════════════════════
    // 3. Accumulateur de statistiques custom
    // ══════════════════════════════════════════════
    println("\n── 3. StatsAccumulator personnalisé ──")

    val statsAcc = new StatsAccumulator
    sc.register(statsAcc, "stats_salaires")

    val salaries = sc.parallelize(Seq(5200.0, 4800.0, 6100.0, 5500.0, 4200.0, 7000.0))
    salaries.foreach(s => statsAcc.add(s))

    val stats = statsAcc.value
    println(f"min=${stats("min")}%.0f  max=${stats("max")}%.0f  " +
            f"sum=${stats("sum")}%.0f  count=${stats("count")}%.0f  avg=${stats("avg")}%.2f")

    // ══════════════════════════════════════════════
    // 4. Piège : double comptage sans cache
    // ══════════════════════════════════════════════
    println("\n── 4. Piège : double comptage ──")

    val trapAcc = sc.longAccumulator("trap")

    val rddLazy = sc.parallelize(1 to 5).map { x =>
      trapAcc.add(1L)
      x * 2
    }

    rddLazy.count()    // 1ère évaluation → trapAcc = 5
    println(s"Après 1er count()   : trapAcc = ${trapAcc.value}")   // 5

    rddLazy.collect()  // 2ème évaluation → trapAcc = 10 ← DOUBLE COMPTAGE !
    println(s"Après collect()     : trapAcc = ${trapAcc.value}  ← DOUBLE COMPTAGE si pas de cache !")

    // ✅ Solution : cache
    val cachedAcc = sc.longAccumulator("cached_count")
    val rddCached = sc.parallelize(1 to 5).map { x =>
      cachedAcc.add(1L)
      x * 2
    }.cache()

    rddCached.count()    // 1ère évaluation → cachedAcc = 5
    println(s"\nAvec cache — après 1er count()  : cachedAcc = ${cachedAcc.value}")

    rddCached.collect()  // lu depuis le cache → cachedAcc inchangé
    println(s"Avec cache — après collect()     : cachedAcc = ${cachedAcc.value}  ✅")
    rddCached.unpersist()

    // ══════════════════════════════════════════════
    // 5. Variables Broadcast — lookup table
    // ══════════════════════════════════════════════
    println("\n── 5. Variables Broadcast ──")

    val countryNames = Map(
      "FR" -> "France",    "UK" -> "Royaume-Uni",
      "DE" -> "Allemagne", "US" -> "États-Unis",
      "ES" -> "Espagne",   "IT" -> "Italie"
    )

    val bcCountries = sc.broadcast(countryNames)

    val employees = sc.parallelize(Seq(
      ("Alice", "FR"), ("Bob", "DE"), ("Carol", "UK"),
      ("Dave",  "US"), ("Eve", "XX"), ("Frank", "IT")
    ))

    val expanded = employees.map { case (name, code) =>
      val country = bcCountries.value.getOrElse(code, s"Inconnu ($code)")
      (name, code, country)
    }

    println("Employés avec pays complet :")
    expanded.collect().foreach { case (n, c, p) =>
      println(s"  $n ($c) → $p")
    }

    // ══════════════════════════════════════════════
    // 6. Broadcast d'une blacklist
    // ══════════════════════════════════════════════
    println("\n── 6. Broadcast d'une blacklist ──")

    val blacklist    = Set("XX", "YY", "ZZ", "AA")
    val bcBlacklist  = sc.broadcast(blacklist)

    val filtered = employees.filter { case (_, code) =>
      !bcBlacklist.value.contains(code)
    }

    println("Employés (codes non blacklistés) :")
    filtered.collect().foreach { case (n, c) => println(s"  $n : $c") }

    // ══════════════════════════════════════════════
    // 7. Broadcast vs closure capturée (comparaison)
    // ══════════════════════════════════════════════
    println("\n── 7. Broadcast vs closure ──")

    val lookupSmall = Map("FR" -> "France", "UK" -> "Royaume-Uni")

    // ❌ Sans broadcast — le Map est sérialisé avec CHAQUE tâche
    val withoutBC = employees.map { case (n, c) =>
      val country = lookupSmall.getOrElse(c, "?")   // closure → sérialisée N fois
      (n, country)
    }

    // ✅ Avec broadcast — sérialisé UNE FOIS par executor
    val bcSmall = sc.broadcast(lookupSmall)
    val withBC  = employees.map { case (n, c) =>
      val country = bcSmall.value.getOrElse(c, "?")
      (n, country)
    }

    println("Sans broadcast : " + withoutBC.collect().map { case (n, c) => s"$n->$c" }.mkString(", "))
    println("Avec broadcast : " + withBC.collect().map { case (n, c) => s"$n->$c" }.mkString(", "))

    // Libérer les variables broadcast
    bcCountries.unpersist()
    bcBlacklist.unpersist()
    bcSmall.destroy()

    spark.stop()
    println("\nDémonstration Accumulateurs & Broadcast terminée.")
  }
}
