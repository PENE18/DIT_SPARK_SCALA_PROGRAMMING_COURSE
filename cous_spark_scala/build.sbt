ThisBuild / scalaVersion := "3.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "cous_spark_scala"
  )

scalaVersion := "2.12.18"

val sparkVersion      = "3.5.1"
val hadoopAwsVersion  = "3.3.4"
val awsSdkVersion     = "1.12.262"
val postgresVersion   = "42.7.3"

libraryDependencies ++= Seq(

  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,

  // S3A
  "org.apache.hadoop" % "hadoop-aws"          % hadoopAwsVersion,
  "com.amazonaws"     % "aws-java-sdk-bundle" % awsSdkVersion,

  // PostgreSQL
  "org.postgresql" % "postgresql" % postgresVersion,

  // Tests
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)


