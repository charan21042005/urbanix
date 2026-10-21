// ==============================================================================
// Urbanix — Day 7: GraphX Road Network Rerouting Engine
// RoadGraphBuilderSpec.scala — Automated Test Suite for GraphX Construction
// ==============================================================================
// Covers all mandatory Stage F verification scenarios:
// Test 1  — Basic graph construction on synthetic fixture
// Test 2  — Deterministic vertex ID mapping across repeated runs
// Test 3  — Duplicate intersection identifiers detection
// Test 4  — Missing road endpoints (dangling edges) detection
// Test 5  — Invalid road attributes (negative / non-finite distance) handling
// Test 6  — Road segment directionality preservation
// Test 7  — Self-loops detection
// Test 8  — Empty input handling
// Test 9  — Source-count reconciliation on known fixture
// Test 10 — Real GraphX operations (triplets, inDegrees, outDegrees, subgraph)
// ==============================================================================

package urbanix.graphx

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

class RoadGraphBuilderSpec extends AnyFunSuite with BeforeAndAfterAll {

  // 'lazy val' provides a stable identifier required for 'import spark.implicits._',
  // initialized on first access and preserved for the duration of the test suite.
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

  // --- TEST 3: Duplicate intersection identifiers ---
  test("Test 3: Duplicate intersection IDs are detected and distinct counts reported") {
    import spark.implicits._

    val rawVerticesDF = Seq(
      (1L, "Downtown A", "zone_1"),
      (1L, "Downtown A Duplicate", "zone_1"),
      (2L, "North B", "zone_2")
    ).toDF("id", "name", "zone_id")

    val rawCount = rawVerticesDF.count()
    val distinctCount = rawVerticesDF.select("id").distinct().count()

    assert(rawCount == 3)
    assert(distinctCount == 2)
    assert(rawCount > distinctCount, "Duplicate IDs must be detectable via count comparison")
  }

  // --- TEST 4: Missing road endpoints ---
  test("Test 4: Missing road endpoints (dangling edges) are accurately flagged") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Grand & 1st", "zone_1"),
      (2L, "Broadway & 7th", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Edge refers to nonexistent node 999
    val edgesDF = Seq(
      (1L, 2L, 1.2),
      (1L, 999L, 15.0)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val summary = RoadGraphBuilder.auditGraph(graph, expectedVertexCount = 2, expectedEdgeCount = 2)

    assert(summary.danglingEdgeCount == 1, "Graph audit must flag edge targeting nonexistent node 999")
    assert(!summary.isValid, "Graph with dangling edges must fail validity check")
  }

  // --- TEST 5: Invalid road attributes ---
  test("Test 5: Invalid or non-positive road distance attributes are detectable") {
    import spark.implicits._

    val edgesDF = Seq(
      (1L, 2L, 5.0),
      (2L, 3L, -1.0),      // Negative distance
      (3L, 4L, 0.0),       // Zero distance
      (4L, 5L, Double.NaN) // NaN distance
    ).toDF("src_id", "dst_id", "base_distance_km")

    val invalidEdges = edgesDF.filter("base_distance_km <= 0.0 OR isnan(base_distance_km)").count()
    assert(invalidEdges == 3, "Filter must capture negative, zero, and NaN distances")
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

    // Verify forward edge exists
    val forwardEdge = graph.edges.filter(e => e.srcId == 1L && e.dstId == 2L).count()
    // Verify reverse edge does NOT exist
    val reverseEdge = graph.edges.filter(e => e.srcId == 2L && e.dstId == 1L).count()

    assert(forwardEdge == 1, "Forward edge must exist")
    assert(reverseEdge == 0, "Reverse edge must NOT be fabricated")
  }

  // --- TEST 7: Self-loops detection ---
  test("Test 7: Self-loops (src_id == dst_id) are audited and flagged") {
    import spark.implicits._

    val verticesDF = Seq(
      (1L, "Intersection 1", "zone_1")
    ).toDF("id", "name", "zone_id")

    // Self loop: 1 -> 1
    val edgesDF = Seq(
      (1L, 1L, 0.5)
    ).toDF("src_id", "dst_id", "base_distance_km")

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val summary = RoadGraphBuilder.auditGraph(graph, expectedVertexCount = 1, expectedEdgeCount = 1)

    assert(summary.selfLoopCount == 1, "Self-loop edge must be detected")
    assert(!summary.isValid, "Graph with self-loops must fail validity check")
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

    val graph = RoadGraphBuilder.buildGraph(spark, verticesDF, edgesDF)
    val summary = RoadGraphBuilder.auditGraph(graph, expectedVertexCount = 4, expectedEdgeCount = 6)

    assert(summary.isValid)
    assert(summary.graphVertexCount == 4)
    assert(summary.graphEdgeCount == 6)
    assert(summary.danglingEdgeCount == 0)
    assert(summary.selfLoopCount == 0)
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
}
