// ==============================================================================
// Urbanix — Day 7: GraphX Road Network Rerouting Engine
// RoadGraphBuilderSpec.scala — Comprehensive Test Suite Exercising Production Validation
// ==============================================================================

package urbanix.graphx

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

class RoadGraphBuilderSpec extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = {
    val s = SparkSession.builder()
      .appName("RoadGraphBuilderSpec")
      .master("local[2]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  // --- TEST 1: Basic graph construction ---
  test("Test 1: Basic graph construction matches fixture vertex and edge counts") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node A", "zone_1"),
      (2L, "Node B", "zone_1"),
      (3L, "Node C", "zone_2")
    ).toDF("id", "name", "zone_id")

    val edgesDF = Seq(
      (1L, 2L, 5.0),
      (2L, 3L, 8.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    assert(graph.vertices.count() == 3)
    assert(graph.edges.count() == 2)
  }

  // --- TEST 2: Deterministic vertex mapping ---
  test("Test 2: Deterministic vertex mapping produces identical IDs across runs") {
    import spark.implicits._

    val verticesDF = Seq(
      (101L, "Intersection 101", "zone_1"),
      (202L, "Intersection 202", "zone_2"),
      (303L, "Intersection 303", "zone_3")
    ).toDF("id", "name", "zone_id")

    val edgesDF = Seq(
      (101L, 202L, 4.2)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph1 = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val graph2 = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    val ids1 = graph1.vertices.map(_._1).collect().sorted
    val ids2 = graph2.vertices.map(_._1).collect().sorted

    assert(ids1.sameElements(ids2))
    assert(ids1.sameElements(Array(101L, 202L, 303L)))
  }

  // --- TEST 3: Production validation of duplicate intersection identifiers ---
  test("Test 3: Production validator flags duplicate intersection IDs and fails source audit") {
    import spark.implicits._

    val rawVerticesDF = Seq(
      (1L, "Downtown A", "zone_1"),
      (1L, "Downtown A Duplicate", "zone_1"),
      (2L, "North B", "zone_2")
    ).toDF("id", "name", "zone_id")

    val validEdgesDF = Seq(
      (1L, 2L, 3.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Exercise actual production validation code
    val sourceReport = RoadGraphBuilder.validateSourceData(rawVerticesDF, validEdgesDF)

    assert(sourceReport.intersectionsTotalRows == 3)
    assert(sourceReport.intersectionsDistinctIdCount == 2)
    assert(sourceReport.intersectionsDuplicateIdCount == 1)
    assert(!sourceReport.isSourceValid, "Validator must reject datasets with duplicate intersection IDs")
    assert(sourceReport.errorMessages.exists(_.contains("duplicate intersection ID")))
  }

  // --- TEST 4: Production audit of missing road endpoints (dangling edges) ---
  test("Test 4: Production graph audit flags dangling edges and GraphX phantom vertices") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Grand & 1st", "zone_1"),
      (2L, "Broadway & 7th", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Edge refers to nonexistent node 999L
    val edgesDF = Seq(
      (1L, 2L, 1.2),
      (1L, 999L, 15.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)

    // Exercise actual production auditGraph method against source DataFrames
    val summary = RoadGraphBuilder.auditGraph(graph, verticesDF, edgesDF)

    assert(summary.danglingEdgeCount == 1, "Graph audit must flag edge targeting nonexistent node 999")
    assert(summary.phantomVertexCount == 1, "GraphX auto-inserts phantom vertex with null attr for node 999")
    assert(!summary.isValid, "Graph with dangling edges must fail validity check")
    assert(summary.diagnostics.exists(_.contains("dangling edges")))
  }

  // --- TEST 5: Production validation of invalid road attributes ---
  test("Test 5: Production validator flags negative, zero, and NaN road distances") {
    import spark.implicits._

    val validVerticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1"),
      (3L, "Node 3", "zone_1"),
      (4L, "Node 4", "zone_1"),
      (5L, "Node 5", "zone_1")
    ).toDF("id", "name", "zone_id")

    val invalidEdgesDF = Seq(
      (1L, 2L, 5.0),
      (2L, 3L, -1.0),      // Negative distance
      (3L, 4L, 0.0),       // Zero distance
      (4L, 5L, Double.NaN) // NaN distance
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Exercise actual production validation code
    val sourceReport = RoadGraphBuilder.validateSourceData(validVerticesDF, invalidEdgesDF)

    assert(sourceReport.roadsInvalidDistanceCount == 3, "Validator must count all 3 invalid distances")
    assert(!sourceReport.isSourceValid, "Validator must reject datasets with invalid road distances")
    assert(sourceReport.errorMessages.exists(_.contains("non-positive, null, NaN, or infinite")))
  }

  // --- TEST 6: Directionality preservation ---
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

    val forwardEdge = graph.edges.filter(e => e.srcId == 1L && e.dstId == 2L).count()
    val reverseEdge = graph.edges.filter(e => e.srcId == 2L && e.dstId == 1L).count()

    assert(forwardEdge == 1, "Forward edge must exist")
    assert(reverseEdge == 0, "Reverse edge must NOT be fabricated")
  }

  // --- TEST 7: Production detection of self-loops ---
  test("Test 7: Production validator and audit flag self-loops (src_id == dst_id)") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Intersection 1", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Self loop: 1 -> 1
    val edgesDF = Seq(
      (1L, 1L, 0.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    // Test pre-construction validator
    val sourceReport = RoadGraphBuilder.validateSourceData(verticesDF, edgesDF)
    assert(sourceReport.roadsSelfLoopCount == 1)
    assert(!sourceReport.isSourceValid)

    // Test post-construction graph audit
    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val summary = RoadGraphBuilder.auditGraph(graph, verticesDF, edgesDF)
    assert(summary.selfLoopCount == 1)
    assert(!summary.isValid)
  }

  // --- TEST 8: Empty input handling ---
  test("Test 8: Empty datasets construct empty graph with 0 vertices and 0 edges") {
    import spark.implicits._

    val emptyVerticesDF = spark.emptyDataset[(Long, String, String)].toDF("id", "name", "zone_id")
    val emptyEdgesDF = spark.emptyDataset[(Long, Long, Double)].toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, emptyVerticesDF, emptyEdgesDF)
    assert(graph.vertices.count() == 0)
    assert(graph.edges.count() == 0)
  }

  // --- TEST 9: Source-count reconciliation ---
  test("Test 9: Source counts reconcile with graph vertices and edges on known fixture") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1"),
      (3L, "Node 3", "zone_2"),
      (4L, "Node 4", "zone_2")
    ).toDF("id", "name", "zone_id")

    val edgesDF = Seq(
      (1L, 2L, 2.0),
      (2L, 1L, 2.0),
      (2L, 3L, 4.5),
      (3L, 2L, 4.5),
      (3L, 4L, 3.1),
      (4L, 3L, 3.1)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val (graph, summary) = RoadGraphBuilder.validateAndBuildGraph(spark, verticesDF, edgesDF, strict = true)

    assert(summary.isValid)
    assert(summary.graphVertexCount == 4)
    assert(summary.graphEdgeCount == 6)
    assert(summary.danglingEdgeCount == 0)
    assert(summary.selfLoopCount == 0)
    assert(summary.weaklyConnectedComponentCount == 1)
    assert(summary.isDirectedSymmetric)
  }

  // --- TEST 10: Real GraphX operations ---
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

    // Operator 1: triplets
    val tripletList = graph.triplets.collect()
    assert(tripletList.length == 3)
    val t1 = tripletList.find(t => t.srcId == 1L && t.dstId == 2L).get
    assert(t1.srcAttr == "Hub A")
    assert(t1.dstAttr == "Spoke B")
    assert(t1.attr == 3.0)

    // Operator 2: outDegrees
    val outDegMap = graph.outDegrees.collect().toMap
    assert(outDegMap(1L) == 2)
    assert(outDegMap(2L) == 1)

    // Operator 3: inDegrees
    val inDegMap = graph.inDegrees.collect().toMap
    assert(inDegMap(3L) == 2)
    assert(inDegMap(2L) == 1)

    // Operator 4: subgraph (filter edges with distance < 5.0)
    val shortRoadsGraph = graph.subgraph(epred = edgeTriplet => edgeTriplet.attr < 5.0)
    assert(shortRoadsGraph.edges.count() == 2)
  }

  // --- TEST 11: Strict builder throws IllegalArgumentException on corrupt input ---
  test("Test 11: Production validateAndBuildGraph throws IllegalArgumentException in strict mode on invalid data") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1")
    ).toDF("id", "name", "zone_id")

    val corruptedEdgesDF = Seq(
      (1L, 2L, -5.0) // Invalid negative distance
    ).toDF("src_id", "dst_id", "base_distance_km")

    val ex = intercept[IllegalArgumentException] {
      RoadGraphBuilder.validateAndBuildGraph(spark, verticesDF, corruptedEdgesDF, strict = true)
    }
    assert(ex.getMessage.contains("Source validation failed"))
  }

  // --- TEST 12: Exact duplicate records vs legitimate reverse-direction edges ---
  test("Test 12: Production validator distinguishes accidental duplicates from valid bidirectional links") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Node 1", "zone_1"),
      (2L, "Node 2", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Legitimate bidirectional edges (1 -> 2 and 2 -> 1)
    val bidirectionalEdgesDF = Seq(
      (1L, 2L, 3.0),
      (2L, 1L, 3.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val reportValid = RoadGraphBuilder.validateSourceData(verticesDF, bidirectionalEdgesDF)
    assert(reportValid.roadsExactDuplicateRecordCount == 0, "Opposite direction (2->1) is NOT a duplicate of (1->2)")
    assert(reportValid.isSourceValid)

    // Accidental exact duplicate edge (1 -> 2 appears twice)
    val accidentalDuplicateEdgesDF = Seq(
      (1L, 2L, 3.0),
      (1L, 2L, 3.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val reportDuplicate = RoadGraphBuilder.validateSourceData(verticesDF, accidentalDuplicateEdgesDF)
    assert(reportDuplicate.roadsExactDuplicateRecordCount == 1, "Repeated directed edge (1->2) must be flagged as duplicate")
    assert(!reportDuplicate.isSourceValid)
  }

  // --- TEST 13: Topology audit of checked-in metropolitan road dataset ---
  test("Test 13: Topology audit on genuine checked-in CSVs verifies strong connectivity via 100% symmetry") {
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

    val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(spark, intersectionsPath, roadsPath)
    val (graph, audit) = RoadGraphBuilder.validateAndBuildGraph(spark, intersectionsDF, roadsDF, strict = true)

    assert(audit.isValid)
    assert(audit.sourceVertexCount == 20)
    assert(audit.graphVertexCount == 20)
    assert(audit.sourceEdgeCount == 40)
    assert(audit.graphEdgeCount == 40)
    assert(audit.danglingEdgeCount == 0)
    assert(audit.phantomVertexCount == 0)
    assert(audit.selfLoopCount == 0)
    assert(audit.weaklyConnectedComponentCount == 1, "Graph must have 1 connected component")
    assert(audit.isDirectedSymmetric, "All 20 road links must be bidirectional (symmetric directed graph)")
  }
}
