// ==============================================================================
// Urbanix — Day 7: GraphX Road Network Rerouting Engine
// RoadGraphBuilderSpec.scala — Comprehensive Test Suite Exercising Production Validation
// ==============================================================================
//
// WHAT THIS TEST SUITE VALIDATES:
// This suite tests the foundation of the Urbanix road network graph engine:
//   1. Correct transformation from tabular CSV/DataFrames into GraphX property graphs.
//   2. Rigorous two-phase validation:
//      - Phase 1: Pre-construction data sanity checks (duplicate IDs, NaN/negative distances, self-loops).
//      - Phase 2: Post-construction topological auditing (dangling edges, phantom vertices, reciprocity).
//   3. Determinism, directionality preservation, and native GraphX distributed operators.
//   4. Full integrity verification against the real 20-intersection, 40-edge city dataset.
//
// HOW SCALATEST WORKS (FOR BEGINNERS):
//   - `AnyFunSuite`: A standard ScalaTest style where tests are written as readable blocks: `test("description") { ... }`.
//   - `BeforeAndAfterAll`: A lifecycle trait that provides `beforeAll()` (runs once before any test)
//     and `afterAll()` (runs once after all tests finish), ideal for managing heavyweight resources like Spark.
//   - `assert(condition, message)`: Verifies boolean expressions, failing the test with an explanatory message if false.
//   - `intercept[Exception] { ... }`: Asserts that a specific block of code throws a particular exception type.
// ==============================================================================

package urbanix.graphx

// --- ScalaTest Framework Imports ---
// AnyFunSuite provides the functional testing DSL (test("...") { ... })
import org.scalatest.funsuite.AnyFunSuite
// BeforeAndAfterAll hooks into the suite lifecycle to initialize and tear down shared resources
import org.scalatest.BeforeAndAfterAll

// --- Apache Spark SQL & GraphX Imports ---
// SparkSession is the central unified entry point for all Spark computation
import org.apache.spark.sql.SparkSession
// Imports core GraphX classes (Graph, VertexId, Edge, EdgeTriplet)
import org.apache.spark.graphx._

/**
 * RoadGraphBuilderSpec: Automated test specification for the GraphX road graph builder.
 */
class RoadGraphBuilderSpec extends AnyFunSuite with BeforeAndAfterAll {

  // --------------------------------------------------------------------------
  // SHARED TEST FIXTURE: LOCAL SPARKSESSION
  // --------------------------------------------------------------------------
  // In Scala, `lazy val` delays evaluation until the field is accessed for the first time.
  // This prevents allocating heavy Spark worker threads until a test actually needs them.
  lazy val spark: SparkSession = {
    // Build a lightweight, local Spark instance for unit testing
    val s = SparkSession.builder()
      .appName("RoadGraphBuilderSpec")
      // "local[2]" runs Spark locally with 2 worker threads (simulating concurrency)
      .master("local[2]")
      // Explicitly bind to local loopback interface (prevents Windows DNS/network lookup delays)
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Disable the web UI (port 4040) to prevent port collision errors during rapid test cycles
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Reduce noisy internal Spark log messages so test output remains clean and readable
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  /**
   * Lifecycle cleanup hook: Automatically invoked once after all tests in this suite complete.
   * Ensures the SparkContext releases memory and halts worker threads cleanly.
   */
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  // ==========================================================================
  // --- TEST 1: Basic graph construction ---
  // ==========================================================================
  test("Test 1: Basic graph construction matches fixture vertex and edge counts") {
    // Importing spark.implicits._ enables .toDF() on Scala collections (Seq, List)
    import spark.implicits._

    // Create a small synthetic DataFrame of 3 intersection vertices
    val verticesDF = Seq(
      (1L, "Node A", "zone_1"),
      (2L, "Node B", "zone_1"),
      (3L, "Node C", "zone_2")
    ).toDF("id", "name", "zone_id")

    // Create a small synthetic DataFrame of 2 directed road segments
    val edgesDF = Seq(
      (1L, 2L, 5.0),
      (2L, 3L, 8.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Build the GraphX property multigraph using our production builder
    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Assert that the constructed graph has exactly 3 vertices and 2 directed edges
    assert(graph.vertices.count() == 3, "Graph must contain exactly 3 vertices")
    assert(graph.edges.count() == 2, "Graph must contain exactly 2 edges")
  }

  // ==========================================================================
  // --- TEST 2: Deterministic vertex mapping ---
  // ==========================================================================
  test("Test 2: Deterministic vertex mapping produces identical IDs across runs") {
    import spark.implicits._

    // Define 3 vertices with specific, non-sequential 64-bit Long IDs
    val verticesDF = Seq(
      (101L, "Intersection 101", "zone_1"),
      (202L, "Intersection 202", "zone_2"),
      (303L, "Intersection 303", "zone_3")
    ).toDF("id", "name", "zone_id")

    // Define a single road connecting two of the nodes
    val edgesDF = Seq(
      (101L, 202L, 4.2)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Construct two independent graph instances from the exact same input DataFrames
    val graph1 = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val graph2 = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Collect and sort vertex IDs from both graphs
    val ids1 = graph1.vertices.map(_._1).collect().sorted
    val ids2 = graph2.vertices.map(_._1).collect().sorted

    // Verify both graphs produce identical, deterministic IDs matching source data
    assert(ids1.sameElements(ids2), "Vertex IDs must be perfectly deterministic across independent runs")
    assert(ids1.sameElements(Array(101L, 202L, 303L)), "Vertex IDs must match source input IDs exactly")
  }

  // ==========================================================================
  // --- TEST 3: Production validation of duplicate intersection identifiers ---
  // ==========================================================================
  test("Test 3: Production validator flags duplicate intersection IDs and fails source audit") {
    import spark.implicits._

    // Intentionally construct invalid data: ID 1L appears twice with different names
    val rawVerticesDF = Seq(
      (1L, "Downtown A", "zone_1"),
      (1L, "Downtown A Duplicate", "zone_1"),
      (2L, "North B", "zone_2")
    ).toDF("id", "name", "zone_id")

    val validEdgesDF = Seq(
      (1L, 2L, 3.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Run Phase 1 source validation against the corrupt input
    val sourceReport = RoadGraphBuilder.validateSourceData(rawVerticesDF, validEdgesDF)

    // Assert that the validator accurately counted rows and flagged duplicates
    assert(sourceReport.intersectionsTotalRows == 3, "Total rows in raw data should be 3")
    assert(sourceReport.intersectionsDistinctIdCount == 2, "Distinct IDs should be 2")
    assert(sourceReport.intersectionsDuplicateIdCount == 1, "Duplicate count should be exactly 1")
    assert(!sourceReport.isSourceValid, "Validator must reject datasets with duplicate intersection IDs")
    assert(sourceReport.errorMessages.exists(_.contains("duplicate intersection ID")), "Error message must mention duplicate ID")
  }

  // ==========================================================================
  // --- TEST 4: Production audit of missing road endpoints (dangling edges) ---
  // ==========================================================================
  test("Test 4: Production graph audit flags dangling edges and GraphX phantom vertices") {
    import spark.implicits._

    // Only nodes 1 and 2 exist in the vertex dataset
    val verticesDF = Seq(
      (1L, "Grand & 1st", "zone_1"),
      (2L, "Broadway & 7th", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Edge refers to nonexistent node 999L (a 'dangling edge')
    val edgesDF = Seq(
      (1L, 2L, 1.2),
      (1L, 999L, 15.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // GraphX automatically inserts phantom vertices with null attributes when edges point to missing nodes
    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Run Phase 2 graph audit to detect topological discrepancies
    val summary = RoadGraphBuilder.auditGraph(graph, verticesDF, edgesDF)

    // Verify the audit detects both the dangling edge and the phantom vertex
    assert(summary.danglingEdgeCount == 1, "Graph audit must flag edge targeting nonexistent node 999")
    assert(summary.phantomVertexCount == 1, "GraphX auto-inserts phantom vertex with null attr for node 999")
    assert(!summary.isValid, "Graph with dangling edges must fail validity check")
    assert(summary.diagnostics.exists(_.contains("dangling edges")), "Diagnostics must explain dangling edge failure")
  }

  // ==========================================================================
  // --- TEST 5: Production validation of invalid road attributes ---
  // ==========================================================================
  test("Test 5: Production validator flags negative, zero, and NaN road distances") {
    import spark.implicits._

    val validVerticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1"),
      (3L, "Node 3", "zone_1"),
      (4L, "Node 4", "zone_1"),
      (5L, "Node 5", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Construct edges with various unphysical distance values:
    val invalidEdgesDF = Seq(
      (1L, 2L, 5.0),       // Valid distance
      (2L, 3L, -1.0),      // Negative distance (physically impossible)
      (3L, 4L, 0.0),       // Zero distance (impossible corridor length)
      (4L, 5L, Double.NaN) // NaN distance (corrupted float)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Run source data validator
    val sourceReport = RoadGraphBuilder.validateSourceData(validVerticesDF, invalidEdgesDF)

    // Verify all 3 unphysical records were caught
    assert(sourceReport.roadsInvalidDistanceCount == 3, "Validator must count all 3 invalid distances")
    assert(!sourceReport.isSourceValid, "Validator must reject datasets with invalid road distances")
    assert(sourceReport.errorMessages.exists(_.contains("non-positive, null, NaN, or infinite")), "Error message must cite distance violation")
  }

  // ==========================================================================
  // --- TEST 6: Directionality preservation ---
  // ==========================================================================
  test("Test 6: Directed edges remain strictly directed and reverse edges are not fabricated") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "OneWay In", "zone_1"),
      (2L, "OneWay Out", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Single one-way directed road 1 -> 2
    val edgesDF = Seq(
      (1L, 2L, 2.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Filter edges in both directions
    val forwardEdge = graph.edges.filter(e => e.srcId == 1L && e.dstId == 2L).count()
    val reverseEdge = graph.edges.filter(e => e.srcId == 2L && e.dstId == 1L).count()

    // Assert that GraphX strictly respects directionality and does NOT auto-generate a return edge
    assert(forwardEdge == 1, "Forward edge (1 -> 2) must exist")
    assert(reverseEdge == 0, "Reverse edge (2 -> 1) must NOT be fabricated")
  }

  // ==========================================================================
  // --- TEST 7: Production detection of self-loops ---
  // ==========================================================================
  test("Test 7: Production validator and audit flag self-loops (src_id == dst_id)") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Intersection 1", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Self loop: edge starts and ends at the exact same node (1 -> 1)
    val edgesDF = Seq(
      (1L, 1L, 0.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Test pre-construction validator (Phase 1)
    val sourceReport = RoadGraphBuilder.validateSourceData(verticesDF, edgesDF)
    assert(sourceReport.roadsSelfLoopCount == 1, "Validator must detect 1 self loop")
    assert(!sourceReport.isSourceValid, "Source validation must fail due to self-loop")

    // Test post-construction graph audit (Phase 2)
    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val summary = RoadGraphBuilder.auditGraph(graph, verticesDF, edgesDF)
    assert(summary.selfLoopCount == 1, "Graph audit must confirm 1 self loop edge")
    assert(!summary.isValid, "Graph audit must declare graph invalid")
  }

  // ==========================================================================
  // --- TEST 8: Empty input handling ---
  // ==========================================================================
  test("Test 8: Empty datasets construct empty graph with 0 vertices and 0 edges") {
    import spark.implicits._

    // Create empty DataFrames using Spark's emptyDataset helper
    val emptyVerticesDF = spark.emptyDataset[(Long, String, String)].toDF("id", "name", "zone_id")
    val emptyEdgesDF = spark.emptyDataset[(Long, Long, Double)].toDF("src_id", "dst_id", "base_distance_km")

    // Build graph from empty data
    val graph = RoadGraphBuilder.buildGraph(spark, emptyVerticesDF, emptyEdgesDF)

    // Verify that empty inputs gracefully produce an empty graph without throwing NullPointerExceptions
    assert(graph.vertices.count() == 0, "Vertex count must be 0")
    assert(graph.edges.count() == 0, "Edge count must be 0")
  }

  // ==========================================================================
  // --- TEST 9: Source-count reconciliation ---
  // ==========================================================================
  test("Test 9: Source counts reconcile with graph vertices and edges on known fixture") {
    import spark.implicits._

    // Create a 4-vertex network
    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1"),
      (3L, "Node 3", "zone_2"),
      (4L, "Node 4", "zone_2")
    ).toDF("id", "name", "zone_id")

    // Create 6 directed edges (3 bidirectional links: 1<->2, 2<->3, 3<->4)
    val edgesDF = Seq(
      (1L, 2L, 2.0),
      (2L, 1L, 2.0),
      (2L, 3L, 4.5),
      (3L, 2L, 4.5),
      (3L, 4L, 3.1),
      (4L, 3L, 3.1)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Run combined validation and build pipeline with strict = true
    val (graph, summary) = RoadGraphBuilder.validateAndBuildGraph(spark, verticesDF, edgesDF, strict = true)

    // Assert that graph audit reconciles 100% with source data
    assert(summary.isValid, "Fixture graph must be valid")
    assert(summary.graphVertexCount == 4, "Vertex count must be 4")
    assert(summary.graphEdgeCount == 6, "Edge count must be 6")
    assert(summary.danglingEdgeCount == 0, "Dangling edge count must be 0")
    assert(summary.selfLoopCount == 0, "Self loop count must be 0")
    assert(summary.weaklyConnectedComponentCount == 1, "Graph must have 1 connected component")
    assert(summary.isDirectedSymmetric, "Graph must have 100% directed symmetry")
  }

  // ==========================================================================
  // --- TEST 10: Real GraphX operations ---
  // ==========================================================================
  test("Test 10: GraphX operators (triplets, inDegrees, outDegrees, subgraph) execute correctly") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Hub A", "zone_1"),
      (2L, "Spoke B", "zone_1"),
      (3L, "Spoke C", "zone_2")
    ).toDF("id", "name", "zone_id")

    val edgesDF = Seq(
      (1L, 2L, 3.0),
      (1L, 3L, 7.0),
      (2L, 3L, 4.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Operator 1: triplets (joins vertices and edges into (srcVertex, edgeAttr, dstVertex))
    val tripletList = graph.triplets.collect()
    assert(tripletList.length == 3, "Must produce 3 edge triplets")
    val t1 = tripletList.find(t => t.srcId == 1L && t.dstId == 2L).get
    assert(t1.srcAttr == "Hub A", "Source attribute must be 'Hub A'")
    assert(t1.dstAttr == "Spoke B", "Destination attribute must be 'Spoke B'")
    assert(t1.attr == 3.0, "Edge attribute must be 3.0 km")

    // Operator 2: outDegrees (counts number of outgoing directed edges per vertex)
    val outDegMap = graph.outDegrees.collect().toMap
    assert(outDegMap(1L) == 2, "Node 1 has 2 outgoing edges (to 2 and 3)")
    assert(outDegMap(2L) == 1, "Node 2 has 1 outgoing edge (to 3)")

    // Operator 3: inDegrees (counts number of incoming directed edges per vertex)
    val inDegMap = graph.inDegrees.collect().toMap
    assert(inDegMap(3L) == 2, "Node 3 has 2 incoming edges (from 1 and 2)")
    assert(inDegMap(2L) == 1, "Node 2 has 1 incoming edge (from 1)")

    // Operator 4: subgraph (filters graph based on edge predicate: only keep roads < 5.0 km)
    val shortRoadsGraph = graph.subgraph(epred = edgeTriplet => edgeTriplet.attr < 5.0)
    assert(shortRoadsGraph.edges.count() == 2, "Filtered subgraph must retain exactly 2 edges (3.0 and 4.0 km)")
  }

  // ==========================================================================
  // --- TEST 11: Strict builder throws IllegalArgumentException on corrupt input ---
  // ==========================================================================
  test("Test 11: Production validateAndBuildGraph throws IllegalArgumentException in strict mode on invalid data") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Corrupt data: negative road distance (-5.0 km)
    val corruptedEdgesDF = Seq(
      (1L, 2L, -5.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // intercept[T] verifies that the enclosed code throws the specified exception type
    val ex = intercept[IllegalArgumentException] {
      RoadGraphBuilder.validateAndBuildGraph(spark, verticesDF, corruptedEdgesDF, strict = true)
    }
    assert(ex.getMessage.contains("Source validation failed"), "Exception message must report source validation failure")
  }

  // ==========================================================================
  // --- TEST 12: Exact duplicate records vs legitimate reverse-direction edges ---
  // ==========================================================================
  test("Test 12: Production validator distinguishes accidental duplicates from valid bidirectional links") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Case A: Legitimate bidirectional edges (1 -> 2 and 2 -> 1)
    val bidirectionalEdgesDF = Seq(
      (1L, 2L, 3.0),
      (2L, 1L, 3.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val reportValid = RoadGraphBuilder.validateSourceData(verticesDF, bidirectionalEdgesDF)
    assert(reportValid.roadsExactDuplicateRecordCount == 0, "Opposite direction (2->1) is NOT a duplicate of (1->2)")
    assert(reportValid.isSourceValid, "Bidirectional links must be considered valid")

    // Case B: Accidental exact duplicate edge (1 -> 2 appears twice with identical endpoints)
    val accidentalDuplicateEdgesDF = Seq(
      (1L, 2L, 3.0),
      (1L, 2L, 3.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val reportDuplicate = RoadGraphBuilder.validateSourceData(verticesDF, accidentalDuplicateEdgesDF)
    assert(reportDuplicate.roadsExactDuplicateRecordCount == 1, "Repeated directed edge (1->2) must be flagged as duplicate")
    assert(!reportDuplicate.isSourceValid, "Accidental duplicate edge records must fail source validation")
  }

  // ==========================================================================
  // --- TEST 13: Topology audit of checked-in metropolitan road dataset ---
  // ==========================================================================
  test("Test 13: Topology audit on genuine checked-in CSVs verifies strong connectivity via 100% symmetry") {
    // Resolve paths to the repository's real data files
    val intersectionsPath = RoadGraphBuilder.resolvePath(Seq(
      "data/intersections.csv",
      "../../data/intersections.csv",
      "../data/intersections.csv"
    ))
    val roadsPath = RoadGraphBuilder.resolvePath(Seq(
      "data/roads.csv",
      "../../data/roads.csv",
      "../data/roads.csv"
    ))

    // Load CSVs into DataFrames
    val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(spark, intersectionsPath, roadsPath)
    // Run two-phase validation and build the production graph
    val (graph, audit) = RoadGraphBuilder.validateAndBuildGraph(spark, intersectionsDF, roadsDF, strict = true)

    // Verify all 10 topological health criteria against production dataset
    assert(audit.isValid, "Real checked-in road network must be 100% valid")
    assert(audit.sourceVertexCount == 20, "Must contain exactly 20 intersections")
    assert(audit.graphVertexCount == 20, "Graph vertices must equal 20")
    assert(audit.sourceEdgeCount == 40, "Must contain exactly 40 directed road edges")
    assert(audit.graphEdgeCount == 40, "Graph edges must equal 40")
    assert(audit.danglingEdgeCount == 0, "Must have 0 dangling edges")
    assert(audit.phantomVertexCount == 0, "Must have 0 phantom vertices")
    assert(audit.selfLoopCount == 0, "Must have 0 self-loops")
    assert(audit.weaklyConnectedComponentCount == 1, "Graph must have exactly 1 connected component")
    assert(audit.isDirectedSymmetric, "All 20 road links must be bidirectional (100% symmetric directed graph)")
  }
}
