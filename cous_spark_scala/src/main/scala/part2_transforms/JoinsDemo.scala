package part2_transforms

/**
 * =============================================================
 *   02 — Jointures : inner, left, right, full, semi, anti, cross
 * =============================================================
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Joins_Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ── Données de test
    val employees = Seq(
      (1, "Alice",  Some(10)), (2, "Bob",   Some(20)), (3, "Carol", Some(30)),
      (4, "Dave",   Some(10)), (5, "Eve",   None)
    ).toDF("emp_id", "name", "dept_id")

    val departments = Seq(
      (10, "Engineering"), (20, "Sales"), (40, "Legal")
    ).toDF("dept_id", "dept_name")

    println("Employés  :"); employees.show()
    println("Départements :"); departments.show()

    // ── INNER JOIN
    println("── INNER JOIN ──")
    employees.join(departments, "dept_id", "inner").show()

    // ── LEFT JOIN
    println("── LEFT JOIN ──")
    employees.join(departments, "dept_id", "left").show()

    // ── RIGHT JOIN
    println("── RIGHT JOIN ──")
    employees.join(departments, "dept_id", "right").show()

    // ── FULL OUTER
    println("── FULL OUTER JOIN ──")
    employees.join(departments, "dept_id", "full").show()

    // ── SEMI JOIN
    println("── SEMI JOIN (employés ayant un dept) ──")
    employees.join(departments, "dept_id", "leftsemi").show()

    // ── ANTI JOIN
    println("── ANTI JOIN (employés sans dept correspondant) ──")
    employees.join(departments, "dept_id", "leftanti").show()

    // ── Gérer les colonnes dupliquées
    println("── Colonnes dupliquées — solution avec alias ──")
    val df1 = employees.alias("e")
    val df2 = departments.alias("d")
    val result = df1.join(df2, col("e.dept_id") === col("d.dept_id"), "inner")
    result.select("e.emp_id", "e.name", "d.dept_name").show()

    spark.stop()
  }
}
