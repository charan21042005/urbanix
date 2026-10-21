// ==============================================================================
// Urbanix — Day 7: GraphX Road Network Rerouting Engine
// build.sbt — Project Definition and Dependency Management
// ==============================================================================
// In Scala, build.sbt plays the exact same role that requirements.txt + setup.py
// play in Python. It declares the project name, version, the Scala compiler
// version to use, and all external library dependencies.

name := "urbanix-graphx-reroute"

version := "0.1.0"

// Binary Compatibility Note:
// Scala 2.12.18 is explicitly pinned because Apache Spark 3.5.0 official binaries
// on Maven Central are compiled against Scala 2.12.
// Mixing Scala 2.13 artifacts with a Scala 2.12 runtime produces fatal binary
// incompatibility errors (NoSuchMethodError / IncompatibleClassChangeError).
scalaVersion := "2.12.18"

// The %% operator tells sbt to append the Scala binary version (e.g., _2.12)
// to the artifact name automatically when resolving from Maven Central.
libraryDependencies ++= Seq(
  // Apache Spark Core: provides SparkContext, RDD abstraction, serialization,
  // cluster task scheduling, and local memory management.
  "org.apache.spark" %% "spark-core" % "3.5.0",

  // Apache Spark SQL: provides SparkSession, DataFrames, Dataset API, and
  // distributed CSV parsing with automatic schema inference.
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Apache Spark GraphX: the core property graph processing engine on Spark.
  // GraphX is JVM/Scala-only and provides Graph[VD, ED], VertexRDD, EdgeRDD,
  // edge triplets, structural operators, and graph algorithms.
  "org.apache.spark" %% "spark-graphx" % "3.5.0",

  // ScalaTest: modern unit and integration testing framework for Scala.
  // The '% Test' scope indicates that this dependency is only included on the
  // test classpath and is omitted from production JAR packaging.
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

// Fork a new JVM instance when executing 'run' or 'test'.
// This ensures that SparkSession shutdowns and static JVM singletons
// do not leak across consecutive test runs or pollute the sbt shell process.
fork := true

// Configure JVM memory limits for local Spark execution
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M"
)

// Clean and legible test reporting in console output
Test / logBuffered := false
