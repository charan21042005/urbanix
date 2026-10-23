// ==============================================================================
// Urbanix — Day 9: End-to-End Forecast-to-Reroute Integration Engine
// RouteRecommenderSpec.scala — Comprehensive Automated Integration Test Suite
// ==============================================================================
//
// WHAT THIS TEST SUITE VALIDATES:
// This suite tests the end-to-end closed-loop bridge connecting Python and Scala:
//   1. Parquet Prediction Extraction & Deterministic Window Selection:
//      - Reads multi-window temporal partitions written by PySpark MLlib streaming jobs.
//      - Uses Window.partitionBy("zone_id").orderBy(col("ordering_time").desc) to pick
//        the single latest forecast deterministically.
//      - Enforces tie-breaking when two records share identical timestamps.
//      - Exercises schema flexibility (supports both predicted_speed_kmph and predicted_speed).
//      - Filters nulls, NaNs, zero, negative, and unphysically high speeds (> 250 km/h).
//      - Gracefully falls back to free-flow heuristics (40 km/h) if Parquet files are missing.
//   2. Destination-Zone Road Edge Reweighting:
//      - Ensures each directed road edge (u -> v) adopts the speed of its destination intersection v.
//      - Confirms fallback edge metrics are accurately reported when a zone is unmonitored.
//   3. Dynamic Congestion-Triggered Detour Rerouting:
//      - Tests that changing zone speeds dynamically flips optimal path choices.
//   4. Parquet Persistence & Overwrite Semantics:
//      - Verifies that RouteRecommender writes Parquet using OVERWRITE mode so downstream
//        dashboard consumers always read an O(1) freshest routing snapshot.
//   5. End-to-End Integration:
//      - Executes a full iteration on genuine checked-in repository datasets.
//
// HOW SCALATEST INTEGRATION TESTING WORKS (FOR BEGINNERS):
//   - `Files.createTempDirectory`: Creates an isolated scratch folder on the local filesystem
//     for reading/writing test Parquet files without mutating project data directories.
//   - `deleteRecursively`: Cleans up temporary test folders upon suite completion.
//   - `df.write.mode("overwrite").parquet(...)`: Writes Spark DataFrames to disk in columnar format.
// ==============================================================================

package urbanix.graphx

// --- ScalaTest Framework Imports ---
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

// --- Apache Spark SQL Core & Types Imports ---
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// --- Apache Spark GraphX Imports ---
import org.apache.spark.graphx._

// --- Java Standard Library IO & Time Imports ---
import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

/**
 * RouteRecommenderSpec: Automated integration test suite exercising the shared Parquet pipeline.
 */
class RouteRecommenderSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // SparkSession reference and temporary directory reference used across test cases
  private var spark: SparkSession = _
  private var tempDir: File = _

  /**
   * Lifecycle setup hook: Automatically executed before any test in this suite runs.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize local Spark engine configured with UTC timezone for deterministic timestamp tests
    spark = SparkSession.builder()
      .appName("RouteRecommenderSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Create an isolated temporary directory for test Parquet datasets
    tempDir = Files.createTempDirectory("urbanix_day9_test_").toFile
  }

  /**
   * Lifecycle teardown hook: Automatically executed after all tests complete.
   */
  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
      // Recursively delete temporary test files from disk to keep development machines clean
      deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Helper utility to recursively delete a directory and all nested files.
   */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) {
        children.foreach(deleteRecursively)
      }
    }
    file.delete()
  }

  // ============================================================================
  // GROUP 1: PREDICTION EXTRACTION & DETERMINISTIC WINDOW SELECTION TESTS
  // ============================================================================

  // --- Test 1: Window ranking determinism ---
  test("Test 1: loadLatestZonePredictions selects the single latest window record deterministically") {
    val s = spark
    import s.implicits._

    val featurePath = new File(tempDir, "test_multi_window_features").getAbsolutePath

    // Create a multi-window temporal dataset simulating continuous streaming inference
    val records = Seq(
      // Zone 1: Older window at 10:00 (50 km/h) vs Newer window at 10:05 (30 km/h)
      ("zone_1", Timestamp.valueOf("2026-10-22 10:00:00"), 50.0, 10L),
      ("zone_1", Timestamp.valueOf("2026-10-22 10:05:00"), 30.0, 15L),
      // Zone 2: Single window
      ("zone_2", Timestamp.valueOf("2026-10-22 10:00:00"), 45.0, 8L),
      // Zone 3: Three windows; latest is 10:10 (60 km/h)
      ("zone_3", Timestamp.valueOf("2026-10-22 10:00:00"), 20.0, 5L),
      ("zone_3", Timestamp.valueOf("2026-10-22 10:05:00"), 40.0, 12L),
      ("zone_3", Timestamp.valueOf("2026-10-22 10:10:00"), 60.0, 20L)
    )

    // Write records to Parquet on disk
    val df = records.toDF("zone_id", "window_start", "predicted_speed_kmph", "traffic_record_count")
    df.write.mode("overwrite").parquet(featurePath)

    // Run the extraction method under test
    val summary = RouteRecommender.loadLatestZonePredictions(spark, featurePath)

    // Verify aggregate summary metrics
    summary.totalRecordsRead shouldBe 6L
    summary.distinctZonesCount shouldBe 3
    summary.isFromFallback shouldBe false

    // Assert that the latest speed is selected, NEVER an older stale window
    summary.zoneSpeeds("zone_1") shouldBe 30.0 // 10:05 window selected
    summary.zoneSpeeds("zone_2") shouldBe 45.0 // 10:00 window selected
    summary.zoneSpeeds("zone_3") shouldBe 60.0 // 10:10 window selected
  }

  // --- Test 2: Deterministic tie-breaking on identical timestamps ---
  test("Test 2: Deterministic tie-breaking on identical timestamps") {
    val s = spark
    import s.implicits._

    val featurePath = new File(tempDir, "test_tie_break_features").getAbsolutePath

    // Two observations for zone_1 with identical timestamps (10:00:00)
    val records = Seq(
      ("zone_1", Timestamp.valueOf("2026-10-22 10:00:00"), 35.0, 10L),
      ("zone_1", Timestamp.valueOf("2026-10-22 10:00:00"), 55.0, 25L) // Higher record count
    )

    val df = records.toDF("zone_id", "window_start", "predicted_speed_kmph", "traffic_record_count")
    df.write.mode("overwrite").parquet(featurePath)

    val summary = RouteRecommender.loadLatestZonePredictions(spark, featurePath)

    // Record with larger traffic_record_count (25L) should win the tie-break deterministically
    summary.zoneSpeeds("zone_1") shouldBe 55.0
  }

  // --- Test 3: Schema flexibility (column aliases) ---
  test("Test 3: Schema flexibility supports predicted_speed alias and prediction_timestamp") {
    val s = spark
    import s.implicits._

    val featurePath = new File(tempDir, "test_alias_features").getAbsolutePath

    // Uses alternative column names: `prediction_timestamp` and `predicted_speed`
    val records = Seq(
      ("zone_1", Timestamp.valueOf("2026-10-22 12:00:00"), 42.0)
    )

    val df = records.toDF("zone_id", "prediction_timestamp", "predicted_speed")
    df.write.mode("overwrite").parquet(featurePath)

    // Verify the loader automatically detects and adapts to the aliased schema
    val summary = RouteRecommender.loadLatestZonePredictions(spark, featurePath)
    summary.distinctZonesCount shouldBe 1
    summary.zoneSpeeds("zone_1") shouldBe 42.0
  }

  // --- Test 4: Defensive filtering of corrupted model forecasts ---
  test("Test 4: Filters invalid, null, NaN, zero, and unphysical speeds (> 250 km/h)") {
    val featurePath = new File(tempDir, "test_invalid_speeds").getAbsolutePath

    // Define explicit nullable schema to allow null entries
    val schema = StructType(Seq(
      StructField("zone_id", StringType, nullable = true),
      StructField("window_start", TimestampType, nullable = true),
      StructField("predicted_speed_kmph", DoubleType, nullable = true),
      StructField("traffic_record_count", LongType, nullable = true)
    ))

    // Construct rows with various edge cases and corruptions
    val rows = Seq(
      Row("zone_1", Timestamp.valueOf("2026-10-22 10:00:00"), null, 1L),       // Null speed
      Row("zone_1", Timestamp.valueOf("2026-10-22 10:05:00"), Double.NaN, 2L), // NaN speed
      Row("zone_1", Timestamp.valueOf("2026-10-22 10:10:00"), -15.0, 3L),      // Negative speed
      Row("zone_1", Timestamp.valueOf("2026-10-22 10:15:00"), 0.0, 4L),        // Zero speed
      Row("zone_1", Timestamp.valueOf("2026-10-22 10:20:00"), 350.0, 5L),      // Unphysical speed (> 250 km/h)
      Row("zone_1", Timestamp.valueOf("2026-10-22 09:00:00"), 48.0, 6L)        // The ONLY physically valid row
    )

    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, schema)
    df.write.mode("overwrite").parquet(featurePath)

    val summary = RouteRecommender.loadLatestZonePredictions(spark, featurePath)

    // All corrupt rows must be discarded, falling back to the 09:00 observation (48.0 km/h)
    summary.distinctZonesCount shouldBe 1
    summary.zoneSpeeds("zone_1") shouldBe 48.0
  }

  // --- Test 5: Safe fallback on missing directories ---
  test("Test 5: Nonexistent or empty feature table defaults to fallback safely without crashing") {
    val nonexistentPath = new File(tempDir, "nonexistent_directory_123").getAbsolutePath

    // Attempting to read a non-existent path should not throw an unhandled exception
    val summary = RouteRecommender.loadLatestZonePredictions(spark, nonexistentPath)

    summary.totalRecordsRead shouldBe 0L
    summary.distinctZonesCount shouldBe 0
    summary.zoneSpeeds shouldBe empty
    summary.isFromFallback shouldBe true
  }

  // ============================================================================
  // GROUP 2: ZONE-TO-EDGE MAPPING & REWEIGHTING TESTS
  // ============================================================================

  // --- Test 6: Destination-zone edge reweighting logic ---
  test("Test 6: Edge travel time is computed using destination intersection's zone") {
    val s = spark
    import s.implicits._

    // Define intersections: Node 1 in zone_1; Node 2 in zone_2
    val intersectionsDF = Seq(
      (1, "Intersection 1", "zone_1"),
      (2, "Intersection 2", "zone_2")
    ).toDF("id", "name", "zone_id")

    // Road connects Node 1 -> Node 2 with physical distance 10.0 km
    val roadsDF = Seq(
      (1, 2, 10.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val vertices = spark.sparkContext.parallelize(Seq((1L, "Int 1"), (2L, "Int 2")))
    val edges = spark.sparkContext.parallelize(Seq(Edge(1L, 2L, 10.0)))
    val rawGraph: Graph[String, Double] = Graph(vertices, edges)

    // Zone 1 speed is 20 km/h; Zone 2 speed is 50 km/h
    val zoneSpeeds = Map("zone_1" -> 20.0, "zone_2" -> 50.0)

    // Reweight graph using destination-zone mapping
    val res: (Graph[String, Double], EdgeMappingSummary) = RouteRecommender.reweightGraphWithPredictions(
      rawGraph = rawGraph,
      intersectionsDF = intersectionsDF,
      roadsDF = roadsDF,
      zoneSpeedMap = zoneSpeeds
    )
    val reweightedGraph = res._1
    val summary = res._2

    summary.totalEdges shouldBe 1L
    summary.predictedEdgesCount shouldBe 1L
    summary.fallbackEdgesCount shouldBe 0L

    // Destination intersection is Node 2, which belongs to zone_2 (speed = 50.0 km/h).
    // Expected travel time = 10.0 km / 50.0 km/h = 0.2000 hours
    val edgeWeight = reweightedGraph.edges.first().attr
    edgeWeight shouldBe (0.20 +- 0.0001)
  }

  // --- Test 7: Free-flow fallback speed when zone prediction is absent ---
  test("Test 7: Missing zone applies FREE_FLOW_FALLBACK_KMPH (40.0 km/h) and tracks fallback metrics") {
    val s = spark
    import s.implicits._

    val intersectionsDF = Seq(
      (1, "Intersection 1", "zone_1"),
      (2, "Intersection 2", "zone_missing") // Zone with no model prediction
    ).toDF("id", "name", "zone_id")

    val roadsDF = Seq((1, 2, 8.0)).toDF("src_id", "dst_id", "base_distance_km")

    val vertices = spark.sparkContext.parallelize(Seq((1L, "Int 1"), (2L, "Int 2")))
    val edges = spark.sparkContext.parallelize(Seq(Edge(1L, 2L, 8.0)))
    val rawGraph: Graph[String, Double] = Graph(vertices, edges)

    val zoneSpeeds = Map("zone_1" -> 30.0) // zone_missing is deliberately absent from map

    val res: (Graph[String, Double], EdgeMappingSummary) = RouteRecommender.reweightGraphWithPredictions(
      rawGraph = rawGraph,
      intersectionsDF = intersectionsDF,
      roadsDF = roadsDF,
      zoneSpeedMap = zoneSpeeds,
      fallbackSpeedKmph = 40.0
    )
    val reweightedGraph = res._1
    val summary = res._2

    // Verify metrics show fallback was utilized
    summary.fallbackEdgesCount shouldBe 1L
    summary.predictedEdgesCount shouldBe 0L

    // Expected travel time = 8.0 km / 40.0 km/h = 0.2000 hours
    val edgeWeight = reweightedGraph.edges.first().attr
    edgeWeight shouldBe (0.20 +- 0.0001)
  }

  // ============================================================================
  // GROUP 3: ROUTING & DYNAMIC CONGESTION REROUTING TESTS
  // ============================================================================

  // --- Test 8: End-to-end dynamic reroute proof ---
  test("Test 8: Controlled congestion change dynamically alters optimal route and cost") {
    val s = spark
    import s.implicits._

    // Day 8 Section 3.1 4-node benchmark:
    // Path 1: A (1) -> B (2) -> D (4)
    // Path 2: A (1) -> C (3) -> D (4)
    val intersectionsDF = Seq(
      (1, "A", "zone_A"),
      (2, "B", "zone_B"),
      (3, "C", "zone_C"),
      (4, "D", "zone_D")
    ).toDF("id", "name", "zone_id")

    val roadsDF = Seq(
      (1, 2, 4.0), // A -> B (4 km)
      (1, 3, 6.0), // A -> C (6 km)
      (2, 4, 3.0), // B -> D (3 km)
      (3, 4, 5.0)  // C -> D (5 km)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")
    ))
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 4.0),
      Edge(1L, 3L, 6.0),
      Edge(2L, 4L, 3.0),
      Edge(3L, 4L, 5.0)
    ))
    val rawGraph: Graph[String, Double] = Graph(vertices, edges)

    // Scenario 1: Approach to B is congested (zone_B speed = 10 km/h)
    // Edge A -> B: 4.0 km / 10.0 km/h = 0.4000 h
    // Edge B -> D: 3.0 km / 45.0 km/h = 0.0667 h -> Total A->B->D = 0.4667 h
    // Edge A -> C: 6.0 km / 50.0 km/h = 0.1200 h
    // Edge C -> D: 5.0 km / 45.0 km/h = 0.1111 h -> Total A->C->D = 0.2311 h (FASTER)
    val zoneSpeedsBaseline = Map(
      "zone_B" -> 10.0, // Slow
      "zone_C" -> 50.0, // Free flow
      "zone_D" -> 45.0
    )

    val (graph1, _) = RouteRecommender.reweightGraphWithPredictions(
      rawGraph, intersectionsDF, roadsDF, zoneSpeedsBaseline
    )
    val routes1 = ShortestPathPregel.computeRoutes(graph1, origin = 1L)
    routes1(4L).path shouldBe List(1L, 3L, 4L)
    routes1(4L).totalTravelTimeHours shouldBe (0.2311 +- 0.001)

    // Scenario 2: Traffic conditions flip! Zone B clears (50 km/h) and Zone C jams (10 km/h)
    // Edge A -> B: 4.0 km / 50.0 km/h = 0.0800 h
    // Edge B -> D: 3.0 km / 45.0 km/h = 0.0667 h -> Total A->B->D = 0.1467 h (FASTER)
    // Edge A -> C: 6.0 km / 10.0 km/h = 0.6000 h
    // Edge C -> D: 5.0 km / 45.0 km/h = 0.1111 h -> Total A->C->D = 0.7111 h (SLOWER)
    val zoneSpeedsFlipped = Map(
      "zone_B" -> 50.0, // Cleared
      "zone_C" -> 10.0, // Severe traffic bottleneck
      "zone_D" -> 45.0
    )

    val (graph2, _) = RouteRecommender.reweightGraphWithPredictions(
      rawGraph, intersectionsDF, roadsDF, zoneSpeedsFlipped
    )
    val routes2 = ShortestPathPregel.computeRoutes(graph2, origin = 1L)

    // The optimal route must dynamically flip from A -> C -> D to A -> B -> D!
    routes2(4L).path shouldBe List(1L, 2L, 4L)
    routes2(4L).totalTravelTimeHours shouldBe (0.1467 +- 0.001)
    routes2(4L).isReachable shouldBe true
  }

  // ============================================================================
  // GROUP 4: ROUTE OUTPUT PARQUET PERSISTENCE & OVERWRITE SEMANTICS
  // ============================================================================

  // --- Test 9: Parquet schema integrity and OVERWRITE mode verification ---
  test("Test 9: computeAndPublishRoutes writes valid schema and enforces OVERWRITE mode") {
    val outputPath = new File(tempDir, "test_recommended_routes").getAbsolutePath
    val vertices = spark.sparkContext.parallelize(Seq((1L, "A"), (2L, "B")))
    val edges = spark.sparkContext.parallelize(Seq(Edge(1L, 2L, 0.25)))
    val testGraph = Graph(vertices, edges)

    // Pass 1: Initial publish of 2 routes
    val df1 = RouteRecommender.computeAndPublishRoutes(
      spark = spark,
      weightedGraph = testGraph,
      originId = 1L,
      outputPath = outputPath,
      activeZonesCount = 2,
      fallbackUsed = false,
      computedAt = Timestamp.valueOf("2026-10-22 11:00:00")
    )

    df1.count() shouldBe 2L
    val readBack1 = spark.read.parquet(outputPath)
    readBack1.count() shouldBe 2L

    // Validate that all columns expected by the Day 10 FastAPI dashboard exist in Parquet schema
    val requiredCols = Set(
      "origin_id", "destination_id", "path", "path_formatted",
      "total_travel_time_hours", "total_travel_time_minutes",
      "is_reachable", "computed_at", "prediction_zones_used", "fallback_applied"
    )
    requiredCols.subsetOf(readBack1.columns.toSet) shouldBe true

    // Pass 2: Second publish with OVERWRITE semantics at a later timestamp
    val df2 = RouteRecommender.computeAndPublishRoutes(
      spark = spark,
      weightedGraph = testGraph,
      originId = 1L,
      outputPath = outputPath,
      activeZonesCount = 2,
      fallbackUsed = false,
      computedAt = Timestamp.valueOf("2026-10-22 11:01:00")
    )

    val readBack2 = spark.read.parquet(outputPath)
    // Row count must NOT double from 2 to 4 (proves atomic overwrite, NOT append mode)
    readBack2.count() shouldBe 2L
    // Verify that the persisted records carry the updated timestamp
    readBack2.filter(col("computed_at") === Timestamp.valueOf("2026-10-22 11:01:00")).count() shouldBe 2L
  }

  // --- Test 10: End-to-end iteration on checked-in datasets ---
  test("Test 10: End-to-end executeIteration on checked-in repository datasets") {
    val outputPath = new File(tempDir, "test_repo_output").getAbsolutePath
    val config = RecommenderConfig(
      featureTablePath = "data/feature_table/traffic",
      intersectionsPath = "data/intersections.csv",
      roadsPath = "data/roads.csv",
      outputPath = outputPath,
      originId = 1L,
      runOnce = true
    )

    // Run complete iteration
    val routeDF = RouteRecommender.executeIteration(spark, config)
    routeDF.count() shouldBe 20L

    // All 20 municipal intersections must be reachable
    val reachableCount = routeDF.filter(col("is_reachable") === true).count()
    reachableCount shouldBe 20L

    // Confirm that the published output is physically written to disk and readable by Parquet readers
    val diskDF = spark.read.parquet(outputPath)
    diskDF.count() shouldBe 20L
  }
}
