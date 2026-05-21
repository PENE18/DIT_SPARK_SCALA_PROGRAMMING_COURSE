package part2_transforms

/**
 * =============================================================
 *   Part 2 — Pipeline complet : Colonnes, Joins, Explode,
 *             When/Otherwise, Union, Pivot, JSON, Bad Records
 * =============================================================
 * Lancer depuis la racine du projet
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FullPipelinePart2 {
  def main(args: Array[String]): Unit = {

    val baseDir = sys.env.getOrElse("PROJECT_ROOT", ".")

    val spark = SparkSession.builder()
      .appName("Part2_FullPipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n" + "=" * 60)
    println("  PART 2 — Transformations avancées")
    println("=" * 60)

    // ══════════════════════════════════════════════════════════
    // 1. ADD / RENAME / DROP COLUMNS
    // ══════════════════════════════════════════════════════════
    println("\n── 1. withColumn / withColumnRenamed / drop ──")

    val csvPath = s"$baseDir/data/csv/employees.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val dfCols = df
      .withColumn("annual_salary", col("salary") * 12)
      .withColumn("salary_band",
        when(col("salary") > 6000, "High")
        .when(col("salary") > 4500, "Mid")
        .otherwise("Low"))
      .withColumn("name_upper", upper(col("name")))
      .withColumnRenamed("country", "nation")
      .drop("hire_date")

    dfCols.show(5)

    // ══════════════════════════════════════════════════════════
    // 2. JOINS
    // ══════════════════════════════════════════════════════════
    println("\n── 2. Jointures ──")

    val deptMapping = Seq(
      ("Engineering", 1), ("Sales", 2), ("HR", 3)
    ).toDF("dept_name", "dept_id")

    val deptPath = s"$baseDir/data/csv/departments.csv"
    val dfDept = spark.read.option("header", "true").option("inferSchema", "true").csv(deptPath)

    val dfWithId = df.join(deptMapping, df("department") === deptMapping("dept_name"), "left")
      .drop("dept_name")

    println("INNER JOIN employees + departments :")
    val dfJoined = dfWithId.join(
      dfDept.select("dept_id", "dept_name", "manager"),
      "dept_id",
      "inner"
    )
    dfJoined.select("id", "name", "salary", "dept_name", "manager").show(5)

    println("LEFT ANTI JOIN — employés sans département correspondant :")
    val dfAnti = dfWithId.join(dfDept, "dept_id", "leftanti")
    dfAnti.select("id", "name", "dept_id").show()

    // ══════════════════════════════════════════════════════════
    // 3. EXPLODE
    // ══════════════════════════════════════════════════════════
    println("\n── 3. Explode ──")

    val dataSkills = Seq(
      (1, "Alice", Seq("Python", "Spark", "SQL")),
      (2, "Bob",   Seq("Java", "Scala")),
      (3, "Carol", Seq.empty[String]),
      (4, "Dave",  null.asInstanceOf[Seq[String]])
    ).toDF("id", "name", "skills")

    println("explode() — supprime les lignes vides/null :")
    dataSkills.withColumn("skill", explode(col("skills"))).show()

    println("explode_outer() — garde les lignes vides (null) :")
    dataSkills.withColumn("skill", explode_outer(col("skills"))).show()

    println("posexplode() — ajoute l'index de position :")
    dataSkills.select(col("id"), col("name"), posexplode(col("skills")).as(Seq("pos", "skill"))).show()

    // ══════════════════════════════════════════════════════════
    // 4. WHEN / OTHERWISE
    // ══════════════════════════════════════════════════════════
    println("\n── 4. when / otherwise ──")

    val dfGrade = Seq(
      (1, "Alice", 92), (2, "Bob", 78), (3, "Carol", 65),
      (4, "Dave",  55), (5, "Eve",  40)
    ).toDF("id", "name", "score")

    val dfGraded = dfGrade
      .withColumn("grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F"))
      .withColumn("mention",
        when(col("score") >= 90, "Très Bien")
        .when(col("score") >= 80, "Bien")
        .when(col("score") >= 70, "Assez Bien")
        .otherwise("Insuffisant"))

    dfGraded.show()

    // ══════════════════════════════════════════════════════════
    // 5. UNION
    // ══════════════════════════════════════════════════════════
    println("\n── 5. Union ──")

    val df1 = Seq((1, "Alice", 3000.0), (2, "Bob", 4000.0)).toDF("id", "name", "salary")
    val df2 = Seq((3, "Carol", 5000.0), (1, "Alice", 3000.0)).toDF("id", "name", "salary")

    println("union() — garde les doublons (comme UNION ALL) :")
    df1.union(df2).show()

    println("union().distinct() — supprime les doublons :")
    df1.union(df2).distinct().show()

    println("unionByName() — correspondance par nom de colonne :")
    val df2Swapped = df2.select("name", "id", "salary")
    df1.unionByName(df2Swapped).show()

    // ══════════════════════════════════════════════════════════
    // 6. PIVOT
    // ══════════════════════════════════════════════════════════
    println("\n── 6. Pivot ──")

    val salesPath = s"$baseDir/data/csv/sales.csv"
    val dfSales = spark.read.option("header", "true").option("inferSchema", "true").csv(salesPath)

    println("Pivot — ventes par région par catégorie :")
    val dfPivot = dfSales
      .groupBy("region")
      .pivot("category", Seq("Electronics", "Furniture", "Accessories"))
      .agg(sum("amount").alias("total"))
    dfPivot.show()

    // ══════════════════════════════════════════════════════════
    // 7. JSON — LECTURE ET APLATISSEMENT
    // ══════════════════════════════════════════════════════════
    println("\n── 7. Lire et aplatir JSON ──")

    val jsonPath = s"$baseDir/data/json/employees_nested.json"
    val dfJson = spark.read.option("multiLine", "true").json(jsonPath)

    println("Schéma JSON (avec structs imbriqués) :")
    dfJson.printSchema()
    println("Aperçu brut :")
    dfJson.show(3, truncate = false)

    println("Aplatissement des champs imbriqués :")
    val dfFlat = dfJson.select(
      col("id"), col("name"), col("age"), col("salary"),
      col("address.city").alias("city"),
      col("address.country").alias("country"),
      col("scores.python").alias("python_score"),
      col("scores.spark").alias("spark_score")
    )
    dfFlat.show()

    println("Explode du tableau de skills :")
    dfJson.select(col("id"), col("name"), explode_outer(col("skills")).alias("skill")).show()

    // ══════════════════════════════════════════════════════════
    // 8. BAD RECORDS HANDLING
    // ══════════════════════════════════════════════════════════
    println("\n── 8. Gestion des enregistrements malformés ──")

    val errorCsv = s"$baseDir/data/csv/employees_with_errors.csv"

    val schemaWithCorrupt = StructType(Seq(
      StructField("id",              IntegerType, nullable = true),
      StructField("name",            StringType,  nullable = true),
      StructField("age",             IntegerType, nullable = true),
      StructField("salary",          DoubleType,  nullable = true),
      StructField("country",         StringType,  nullable = true),
      StructField("_corrupt_record", StringType,  nullable = true)
    ))

    println("Mode PERMISSIVE — garde les lignes corrompues dans _corrupt_record :")
    val dfPermCsv = spark.read
      .schema(schemaWithCorrupt)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .csv(errorCsv)
    dfPermCsv.show(truncate = false)

    println("Lignes OK :")
    dfPermCsv.filter(col("_corrupt_record").isNull).drop("_corrupt_record").show()

    println("Lignes corrompues :")
    dfPermCsv.filter(col("_corrupt_record").isNotNull).show(truncate = false)

    println("Mode DROPMALFORMED — supprime silencieusement les lignes malformées :")
    val dfDropCsv = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .csv(errorCsv)
    dfDropCsv.show()
    println(s"  Lignes conservées : ${dfDropCsv.count()}")

    // ══════════════════════════════════════════════════════════
    // Fin
    // ══════════════════════════════════════════════════════════
    spark.stop()
    println("\n🛑 SparkSession arrêtée. Pipeline Part 2 terminé !\n")
  }
}
