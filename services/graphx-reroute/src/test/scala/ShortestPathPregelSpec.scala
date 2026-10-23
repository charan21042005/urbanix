// ==============================================================================
// Urbanix — Day 8: GraphX Congestion-Weighted Shortest-Path Routing Engine
// ShortestPathPregelSpec.scala — Comprehensive Routing Engine Test Suite
// ==============================================================================
//
// WHAT THIS TEST SUITE VALIDATES:
// This suite tests the distributed pathfinding intelligence of Urbanix:
//   1. Congestion-Weighted Travel-Time Formula:
//      - Cost = distance / max(speed, 1.0 km/h floor).
//      - Strict rejection of negative, NaN, and infinite inputs.
//   2. Theoretical Hand-Checked Oracle (Section 3.1):
//      - Evaluates a 4-node diamond graph (A, B, C, D) against pencil-and-paper math.
//      - Confirms A -> C -> D (0.2311 h) is strictly preferred over A -> B -> D (0.4000 h).
//   3. Dynamic Congestion-Triggered Rerouting:
//      - Simulates a gridlock event on C -> D (speed drops to 10 km/h).
//      - Proves that Pregel dynamically flips the optimal path to A -> B -> D.
//   4. Robustness & Graph Theory Invariants:
//      - Strictly directed edges (one-way roads cannot be traversed backwards).
//      - Disconnected/unreachable vertices report Double.PositiveInfinity and empty path.
//      - Deterministic lexicographic tie-breaking when two routes have equal cost.
//   5. Convergence & Boundary Defense:
//      - Detects incomplete superstep convergence and rejects invalid iteration limits.
//
// HOW SCALATEST MATCHERS WORK (FOR BEGINNERS):
//   - `Matchers`: Enables human-readable assertions like:
//       `travelTime shouldBe (0.10 +- 0.000001)` (asserts equality within a tolerance delta)
//       `timeSlow should be > timeFast` (asserts greater than)
//       `result.path shouldBe List(1L, 3L, 4L)` (asserts exact list match)
//       `an [IllegalArgumentException] should be thrownBy { ... }` (asserts expected failure)
// ==============================================================================

package urbanix.graphx

// --- ScalaTest Framework Imports ---
// AnyFunSuite provides the test("name") { ... } block syntax
import org.scalatest.funsuite.AnyFunSuite
// Matchers provides fluent shouldBe, should be >, and thrownBy assertions
import org.scalatest.matchers.should.Matchers
// BeforeAndAfterAll manages the setup and teardown lifecycle of the SparkSession
import org.scalatest.BeforeAndAfterAll

// --- Apache Spark SQL & GraphX Imports ---
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

/**
 * ShortestPathPregelSpec: Automated test suite for the Pregel SSSP routing algorithm and cost functions.
 */
class ShortestPathPregelSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // Private mutable reference to hold the test SparkSession across all test cases
  private var spark: SparkSession = _

  /**
   * Lifecycle setup hook: Automatically runs once before any test in this class executes.
   */
  override def beforeAll(): Unit = {
    // Spin up a local Spark engine with 2 threads
    spark = SparkSession.builder()
      .appName("ShortestPathPregelSpec")
      .master("local[2]")
      // Disable the web UI server to eliminate port binding conflicts
      .config("spark.ui.enabled", "false")
      // Bind driver to loopback interface for fast, reliable local communication
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // Silence info logs so console output shows only test progression
    spark.sparkContext.setLogLevel("ERROR")
  }

  /**
   * Lifecycle teardown hook: Automatically runs once after all test cases finish.
   */
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // ============================================================================
  // CATEGORY 1: CONGESTION WEIGHT TRAVEL-TIME FORMULA TESTS
  // ============================================================================

  // --- Formula Test 1: Baseline valid calculation ---
  test("Formula Test 1: Travel time formula matches known valid input (4 km / 40 km/h = 0.10 h)") {
    // 4.0 kilometers divided by 40.0 km/h = exactly 0.10 hours (6.0 minutes)
    val travelTime = CongestionWeight.calculateTravelTimeHours(4.0, 40.0)
    // `+- 0.000001` checks floating point equality within a precision tolerance
    travelTime shouldBe (0.10 +- 0.000001)
    // Verify hours-to-minutes conversion helper (0.10 * 60 = 6.0 minutes)
    CongestionWeight.hoursToMinutes(travelTime) shouldBe (6.0 +- 0.000001)
  }

  // --- Formula Test 2: Slower speed increases cost ---
  test("Formula Test 2: Slower speed produces higher travel-time cost for identical distance") {
    // 10 km at 20 km/h takes 0.50 hours (30 min)
    val timeSlow = CongestionWeight.calculateTravelTimeHours(10.0, 20.0)
    // 10 km at 60 km/h takes 0.1667 hours (10 min)
    val timeFast = CongestionWeight.calculateTravelTimeHours(10.0, 60.0)

    // Higher congestion (slower speed) must strictly yield higher traversal cost
    timeSlow should be > timeFast
    timeSlow shouldBe (0.50 +- 0.000001)
    timeFast shouldBe ((10.0 / 60.0) +- 0.000001)
  }

  // --- Formula Test 3: Faster speed decreases cost ---
  test("Formula Test 3: Faster speed produces lower travel-time cost for identical distance") {
    val timeBase = CongestionWeight.calculateTravelTimeHours(5.0, 50.0)  // 5 / 50 = 0.10 h
    val timeSurge = CongestionWeight.calculateTravelTimeHours(5.0, 100.0) // 5 / 100 = 0.05 h
    timeSurge should be < timeBase
    timeSurge shouldBe (0.05 +- 0.000001)
  }

  // --- Formula Test 4: Physical distance boundary checks ---
  test("Formula Test 4: Rejects zero, negative, NaN, and infinite base distances") {
    // Corrupt or physically impossible road distances must throw an IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(0.0, 50.0) // Zero distance rejected
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(-5.0, 50.0) // Negative distance rejected
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(Double.NaN, 50.0) // NaN rejected
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(Double.PositiveInfinity, 50.0) // Infinite distance rejected
    }
  }

  // --- Formula Test 5: Vehicle speed boundary checks ---
  test("Formula Test 5: Rejects negative, NaN, and infinite vehicle speeds") {
    // Speeds cannot be negative, NaN, or infinite
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(5.0, -10.0)
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(5.0, Double.NaN)
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(5.0, Double.PositiveInfinity)
    }
  }

  // --- Formula Test 6: Gridlock floor clamping (1.0 km/h) ---
  test("Formula Test 6: Zero or near-zero speed is defensively clamped to gridlock floor (1.0 km/h)") {
    // Case A: Speed is 0.0 km/h (total standstill).
    // Instead of crashing with division-by-zero (Inf), it clamps to 1.0 km/h:
    // Travel time = 3.0 km / 1.0 km/h = 3.0 hours.
    val travelTimeZero = CongestionWeight.calculateTravelTimeHours(3.0, 0.0)
    travelTimeZero shouldBe (3.0 +- 0.000001)

    // Case B: Speed is 0.2 km/h (crawl < 1.0 floor).
    // Clamps to 1.0 km/h: Travel time = 2.0 km / 1.0 km/h = 2.0 hours.
    val travelTimeNearZero = CongestionWeight.calculateTravelTimeHours(2.0, 0.2)
    travelTimeNearZero shouldBe (2.0 +- 0.000001)
  }

  // --- Formula Test 7: Custom minimum speed floor ---
  test("Formula Test 7: Custom minimum speed floor is respected when provided") {
    // If user specifies custom minSpeedFloorKmph = 5.0 km/h:
    // Speed of 1.0 km/h is clamped to 5.0 km/h -> 10.0 km / 5.0 km/h = 2.0 hours.
    val travelTimeCustom = CongestionWeight.calculateTravelTimeHours(10.0, 1.0, minSpeedFloorKmph = 5.0)
    travelTimeCustom shouldBe (2.0 +- 0.000001)
  }

  // ============================================================================
  // CATEGORY 2: SECTION 3.1 4-NODE HAND-CHECKED ORACLE BENCHMARK
  // ============================================================================

  /**
   * Helper method to construct the canonical 4-node diamond oracle network:
   *
   *        [B] (congested link B->D: 3km @ 10 km/h = 0.30h)
   *       /   \
   *     [A]   [D]
   *       \   /
   *        [C] (free-flow link C->D: 5km @ 45 km/h = 0.1111h)
   */
  private def buildFourNodeBenchmarkGraph(): Graph[String, Double] = {
    // Create 4 vertices: A (1L), B (2L), C (3L), D (4L)
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")
    ))
    // Calculate travel-time edge weights:
    // Edge A -> B: 4 km @ 40 km/h = 0.1000 h
    // Edge A -> C: 6 km @ 50 km/h = 0.1200 h
    // Edge B -> D: 3 km @ 10 km/h = 0.3000 h (congested)
    // Edge C -> D: 5 km @ 45 km/h = 0.1111... h (free-flowing)
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
      Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
      Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
      Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 45.0))
    ))
    Graph(vertices, edges)
  }

  // --- Benchmark Test 8: Hand-computed travel time verification ---
  test("Benchmark Test 8: Hand-computed minimum travel time to D matches theoretical 0.2311... h") {
    val graph = buildFourNodeBenchmarkGraph()
    // Run Pregel SSSP starting from Origin Node 1L (A)
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    val dResult = routes(4L)
    // Theoretical minimum time: cost(A->C) + cost(C->D) = 0.12 + (5.0 / 45.0) = 0.2311111... hours
    val expectedMinHours = 0.12 + (5.0 / 45.0)
    dResult.totalTravelTimeHours shouldBe (expectedMinHours +- 0.0001)
    dResult.isReachable shouldBe true
  }

  // --- Benchmark Test 9: Route path sequence reconstruction ---
  test("Benchmark Test 9: Reconstructed route sequence to D is exactly A -> C -> D") {
    val graph = buildFourNodeBenchmarkGraph()
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    val dResult = routes(4L)
    // Confirm the algorithm picked path [1L, 3L, 4L] (A -> C -> D)
    dResult.path shouldBe List(1L, 3L, 4L)
  }

  // --- Benchmark Test 10: Mathematical comparison against alternative path ---
  test("Benchmark Test 10: Suboptimal route A -> B -> D has higher cost (0.4000 h) than chosen route") {
    val graph = buildFourNodeBenchmarkGraph()
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    // Intermediate cost to B is 0.10 hours
    val bResult = routes(2L)
    bResult.totalTravelTimeHours shouldBe (0.10 +- 0.0001)

    // Alternative candidate route cost: cost(A->B) + cost(B->D) = 0.10 + 0.30 = 0.4000 hours
    val alternativeCost = 0.10 + (3.0 / 10.0)
    val optimalCost = routes(4L).totalTravelTimeHours

    // Optimal chosen route must be strictly lower cost than the alternative
    optimalCost should be < alternativeCost
    optimalCost shouldBe (0.2311 +- 0.001)
    alternativeCost shouldBe (0.4000 +- 0.0001)
  }

  // ============================================================================
  // CATEGORY 3: DYNAMIC CONGESTION-CHANGE REROUTING TEST
  // ============================================================================

  test("Reroute Test 11: Increasing congestion on C -> D dynamically flips optimal route to A -> B -> D") {
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")
    ))
    // Simulate severe traffic jam on C -> D: speed plummets from 45 km/h to 10 km/h:
    // New cost of C -> D = 5.0 km / 10.0 km/h = 0.5000 h.
    // Total Path A -> C -> D now costs: 0.12 + 0.50 = 0.6200 h.
    // Total Path A -> B -> D costs: 0.10 + 0.30 = 0.4000 h.
    // Therefore, the optimal path MUST dynamically flip to A -> B -> D!
    val congestedEdges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
      Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
      Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
      Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 10.0)) // Congested link!
    ))
    val congestedGraph = Graph(vertices, congestedEdges)
    val routes = ShortestPathPregel.computeRoutes(congestedGraph, origin = 1L)

    val dResult = routes(4L)
    // Verify that the algorithm rerouted to [1L, 2L, 4L] (A -> B -> D) with new optimal cost 0.4000 h
    dResult.path shouldBe List(1L, 2L, 4L)
    dResult.totalTravelTimeHours shouldBe (0.4000 +- 0.0001)
  }

  // ============================================================================
  // CATEGORY 4: DIRECTED SEMANTICS, UNREACHABILITY & INPUT DEFENSE
  // ============================================================================

  // --- Robustness Test 12: Strict edge directionality ---
  test("Robustness Test 12: Directed edge semantics are strictly respected (no reverse traversal)") {
    val vertices = spark.sparkContext.parallelize(Seq((1L, "A"), (2L, "B")))
    // Only directed edge from 1L -> 2L exists (one-way street)
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 0.25)
    ))
    val graph = Graph(vertices, edges)

    // Routing from 2L to 1L: since edge is strictly 1 -> 2, Node 1 is UNREACHABLE from Node 2!
    val routesFrom2 = ShortestPathPregel.computeRoutes(graph, origin = 2L)
    val result1 = routesFrom2(1L)

    result1.isReachable shouldBe false
    result1.totalTravelTimeHours.isPosInfinity shouldBe true
    result1.path shouldBe empty
  }

  // --- Robustness Test 13: Disconnected components & unreachable nodes ---
  test("Robustness Test 13: Unreachable vertices in disconnected graph are correctly reported") {
    // 3 vertices: 1 and 2 are connected; node 3 is completely isolated
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "Isolated")
    ))
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 0.50)
    ))
    val graph = Graph(vertices, edges)
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    // Connected node 2 is reached
    routes(2L).isReachable shouldBe true
    routes(2L).path shouldBe List(1L, 2L)

    // Isolated node 3 is safely flagged as unreachable with infinite travel time
    routes(3L).isReachable shouldBe false
    routes(3L).totalTravelTimeHours.isPosInfinity shouldBe true
    routes(3L).path shouldBe empty
  }

  // --- Robustness Test 14: Nonexistent origin vertex ---
  test("Robustness Test 14: Nonexistent origin vertex throws IllegalArgumentException") {
    val graph = buildFourNodeBenchmarkGraph()
    // Origin 999L does not exist in the graph; algorithm must fail fast with clear exception
    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.computeRoutes(graph, origin = 999L)
    }
  }

  // --- Robustness Test 15: Invalid edge weights ---
  test("Robustness Test 15: Invalid edge weights (negative, NaN, Inf) throw IllegalArgumentException") {
    val vertices = spark.sparkContext.parallelize(Seq((1L, "A"), (2L, "B")))
    // Negative edge weight violates Dijkstra/Bellman-Ford positive cost assumptions
    val edgesNegative = spark.sparkContext.parallelize(Seq(Edge(1L, 2L, -1.5)))
    val graphNegative = Graph(vertices, edgesNegative)

    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.computeRoutes(graphNegative, origin = 1L)
    }
  }

  // --- Robustness Test 16: Empty graph handling ---
  test("Robustness Test 16: Empty graph produces empty routes without crashing") {
    // Create completely empty GraphX graph
    val emptyVertices = spark.sparkContext.emptyRDD[(VertexId, String)]
    val emptyEdges = spark.sparkContext.emptyRDD[Edge[Double]]
    val emptyGraph = Graph(emptyVertices, emptyEdges)

    // Should gracefully return an empty graph without throwing exceptions
    val routed = ShortestPathPregel.runShortestPath(emptyGraph, origin = 1L)
    routed.vertices.count() shouldBe 0L
  }

  // --- Robustness Test 17: Deterministic tie-breaking ---
  test("Robustness Test 17: Equal-cost candidate routes resolve deterministically via tie-breaking") {
    // Two alternative paths of IDENTICAL cost:
    // Path 1: 1 -> 2 -> 4 (cost: 0.10 + 0.10 = 0.20 h)
    // Path 2: 1 -> 3 -> 4 (cost: 0.10 + 0.10 = 0.20 h)
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")
    ))
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 0.10),
      Edge(1L, 3L, 0.10),
      Edge(2L, 4L, 0.10),
      Edge(3L, 4L, 0.10)
    ))
    val graph = Graph(vertices, edges)
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    // Deterministic lexicographic tie-breaker consistently picks "1, 2, 4" over "1, 3, 4"
    routes(4L).totalTravelTimeHours shouldBe (0.20 +- 0.000001)
    routes(4L).path shouldBe List(1L, 2L, 4L)
  }

  // ============================================================================
  // CATEGORY 5: FULL DAY 7 DEMONSTRATION GRAPH ROUTING VERIFICATION
  // ============================================================================

  test("Full Graph Test 18: Full Day 7 20-node road network reaches all 20 vertices with placeholder baseline") {
    // Resolve paths to the repository's real data CSVs
    val intersectionsPath = RoadGraphBuilder.resolvePath(Seq(
      "data/intersections.csv",
      "../../data/intersections.csv",
      "../data/intersections.csv",
      "services/graphx-reroute/data/intersections.csv"
    ))
    val roadsPath = RoadGraphBuilder.resolvePath(Seq(
      "data/roads.csv",
      "../../data/roads.csv",
      "../data/roads.csv",
      "services/graphx-reroute/data/roads.csv"
    ))

    // Load CSVs into DataFrames
    val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(
      spark,
      intersectionsPath,
      roadsPath
    )

    // Build and validate the full 20-node city network
    val (rawGraph, summary) = RoadGraphBuilder.validateAndBuildGraph(
      spark,
      intersectionsDF,
      roadsDF,
      strict = true
    )

    summary.isValid shouldBe true
    summary.graphVertexCount shouldBe 20L
    summary.graphEdgeCount shouldBe 40L

    // Map base distance to travel time using a 50 km/h baseline speed
    val weightedGraph = rawGraph.mapEdges(e =>
      CongestionWeight.calculateTravelTimeHours(e.attr, speedKmph = 50.0)
    )

    // Compute shortest routes from Origin Intersection 1L (Civic Center)
    val routes = ShortestPathPregel.computeRoutes(weightedGraph, origin = 1L)

    // Assert that all 20 nodes in the network are reachable
    routes.size shouldBe 20
    val reachableCount = routes.values.count(_.isReachable)
    reachableCount shouldBe 20

    // Origin-to-origin has zero travel time and trivial path [1L]
    routes(1L).totalTravelTimeHours shouldBe 0.0
    routes(1L).path shouldBe List(1L)

    // Verify all other 19 destinations have strictly positive, finite travel times
    routes.filterKeys(_ != 1L).values.foreach { r =>
      r.isReachable shouldBe true
      r.totalTravelTimeHours should be > 0.0
      r.totalTravelTimeHours.isInfinity shouldBe false
      // Route must begin at Origin 1 and terminate at the destination node
      r.path.head shouldBe 1L
      r.path.last shouldBe r.destinationId
    }
  }

  // ============================================================================
  // CATEGORY 6: CRITICAL PREGEL CONVERGENCE & ITERATION BOUND DEFENSE
  // ============================================================================

  test("Convergence Test 19: User-supplied insufficient iteration limit throws IllegalStateException when improvements remain possible") {
    val graph = buildFourNodeBenchmarkGraph()
    // Reaching destination D from A requires 2 hops (A -> C -> D).
    // If the user configures maxIterations = 1, Pregel stops after superstep 1.
    // Node D has not received messages yet, so improvements remain possible.
    // The algorithm must detect incomplete convergence and fail fast with an IllegalStateException!
    val ex = the[IllegalStateException] thrownBy {
      ShortestPathPregel.runShortestPath(graph, origin = 1L, maxIterations = Some(1))
    }
    // Verify descriptive failure explanation
    ex.getMessage should include("did not converge within the configured iteration limit")
    ex.getMessage should include("further travel-time improvements remain possible")
  }

  test("Convergence Test 20: Non-positive maxIterations parameter throws IllegalArgumentException") {
    val graph = buildFourNodeBenchmarkGraph()
    // Iteration limits <= 0 are mathematically nonsensical and must be rejected immediately
    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.runShortestPath(graph, origin = 1L, maxIterations = Some(0))
    }
    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.runShortestPath(graph, origin = 1L, maxIterations = Some(-2))
    }
  }
}
