package part2_transforms

/**
 * =============================================================
 *   04 — when / otherwise : logique conditionnelle (CASE WHEN)
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WhenOtherwiseDemo {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")
    val csvPath = s"$baseDir/data/csv/employees.csv"

    val spark = SparkSession.builder()
      .appName("When_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    // ── Condition simple
    println("\n── Condition simple ──")
    df.withColumn("senior",
      when(col("age") >= 50, "Senior").otherwise("Non-Senior")
    ).select("name", "age", "senior").show(5)

    // ── Conditions multiples
    println("\n── Tranches de salaire ──")
    df.withColumn("tranche",
      when(col("salary") > 6000, "Haut")
      .when(col("salary") > 4500, "Moyen")
      .when(col("salary") > 3000, "Bas")
      .otherwise("Très Bas")
    ).select("name", "salary", "tranche").show()

    // ── Conditions combinées (AND / OR)
    println("\n── Bonus selon département + salaire ──")
    df.withColumn("bonus",
      when((col("department") === "Engineering") && (col("salary") > 5000), col("salary") * 0.20)
      .when((col("department") === "Sales") || (col("age") > 55), col("salary") * 0.15)
      .otherwise(col("salary") * 0.05)
    ).select("name", "department", "salary", "bonus").show()

    // ── Gestion des nulls avec when
    println("\n── Nettoyage des nulls ──")
    val dataNulls = Seq(
      (1, "Alice", Some(5000.0)),
      (2, "Bob",   None.asInstanceOf[Option[Double]]),
      (3, "Carol", Some(Double.NaN))
    ).toDF("id", "name", "salary")

    dataNulls.withColumn("salary_clean",
      when(col("salary").isNull, lit(0.0))
      .when(isnan(col("salary")),  lit(0.0))
      .otherwise(col("salary"))
    ).show()

    // ── when sans otherwise → null pour les non-matchés
    println("\n── Sans otherwise → null ──")
    df.withColumn("prime",
      when(col("department") === "Engineering", 1000.0)
      // Pas de otherwise → null pour les autres départements
    ).select("name", "department", "prime").show()

    // ── when dans select()
    println("\n── when dans select() ──")
    df.select(
      col("name"),
      col("salary"),
      when(col("salary") > 5000, "Haut")
      .when(col("salary") > 4000, "Moyen")
      .otherwise("Bas")
      .alias("tranche_salaire")
    ).show()

    spark.stop()
  }
}
