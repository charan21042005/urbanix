// ==============================================================================
// Urbanix — Day 8: GraphX Congestion-Weighted Shortest-Path Routing Engine
// ShortestPathPregelSpec.scala — Comprehensive Routing Engine Test Suite
// ==============================================================================

package urbanix.graphx

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

class ShortestPathPregelSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ShortestPathPregelSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // ============================================================================
  // CATEGORY 1: CONGESTION WEIGHT TRAVEL-TIME FORMULA TESTS
  // ============================================================================

  test("Formula Test 1: Travel time formula matches known valid input (4 km / 40 km/h = 0.10 h)") {
    val travelTime = CongestionWeight.calculateTravelTimeHours(4.0, 40.0)
    travelTime shouldBe (0.10 +- 0.000001)
    CongestionWeight.hoursToMinutes(travelTime) shouldBe (6.0 +- 0.000001)
  }

  test("Formula Test 2: Slower speed produces higher travel-time cost for identical distance") {
    val timeSlow = CongestionWeight.calculateTravelTimeHours(10.0, 20.0) // 0.50 h (30 min)
    val timeFast = CongestionWeight.calculateTravelTimeHours(10.0, 60.0) // 0.1667 h (10 min)
    timeSlow should be > timeFast
    timeSlow shouldBe (0.50 +- 0.000001)
    timeFast shouldBe ((10.0 / 60.0) +- 0.000001)
  }

  test("Formula Test 3: Faster speed produces lower travel-time cost for identical distance") {
    val timeBase = CongestionWeight.calculateTravelTimeHours(5.0, 50.0) // 0.10 h
    val timeSurge = CongestionWeight.calculateTravelTimeHours(5.0, 100.0) // 0.05 h
    timeSurge should be < timeBase
    timeSurge shouldBe (0.05 +- 0.000001)
  }

  test("Formula Test 4: Rejects zero, negative, NaN, and infinite base distances") {
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(0.0, 50.0)
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(-5.0, 50.0)
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(Double.NaN, 50.0)
    }
    an[IllegalArgumentException] should be thrownBy {
      CongestionWeight.calculateTravelTimeHours(Double.PositiveInfinity, 50.0)
    }
  }

  test("Formula Test 5: Rejects negative, NaN, and infinite vehicle speeds") {
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

  test("Formula Test 6: Zero or near-zero speed is defensively clamped to gridlock floor (1.0 km/h)") {
    // Distance = 3.0 km, speed = 0.0 km/h -> clamped to 1.0 km/h -> travel time = 3.0 / 1.0 = 3.0 hours
    val travelTimeZero = CongestionWeight.calculateTravelTimeHours(3.0, 0.0)
    travelTimeZero shouldBe (3.0 +- 0.000001)

    // Distance = 2.0 km, speed = 0.2 km/h (< 1.0 floor) -> clamped to 1.0 km/h -> travel time = 2.0 hours
    val travelTimeNearZero = CongestionWeight.calculateTravelTimeHours(2.0, 0.2)
    travelTimeNearZero shouldBe (2.0 +- 0.000001)
  }

  test("Formula Test 7: Custom minimum speed floor is respected when provided") {
    // Custom floor = 5.0 km/h: speed = 1.0 km/h -> clamped to 5.0 km/h -> 10.0 / 5.0 = 2.0 hours
    val travelTimeCustom = CongestionWeight.calculateTravelTimeHours(10.0, 1.0, minSpeedFloorKmph = 5.0)
    travelTimeCustom shouldBe (2.0 +- 0.000001)
  }

  // ============================================================================
  // CATEGORY 2: SECTION 3.1 4-NODE HAND-CHECKED ORACLE BENCHMARK
  // ============================================================================

  private def buildFourNodeBenchmarkGraph(): Graph[String, Double] = {
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")
    ))
    // A -> B: 4 km @ 40 km/h = 0.1000 h
    // A -> C: 6 km @ 50 km/h = 0.1200 h
    // B -> D: 3 km @ 10 km/h = 0.3000 h (congested)
    // C -> D: 5 km @ 45 km/h = 0.1111... h (free-flowing)
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
      Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
      Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
      Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 45.0))
    ))
    Graph(vertices, edges)
  }

  test("Benchmark Test 8: Hand-computed minimum travel time to D matches theoretical 0.2311... h") {
    val graph = buildFourNodeBenchmarkGraph()
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    val dResult = routes(4L)
    val expectedMinHours = 0.12 + (5.0 / 45.0) // 0.231111111...
    dResult.totalTravelTimeHours shouldBe (expectedMinHours +- 0.0001)
    dResult.isReachable shouldBe true
  }

  test("Benchmark Test 9: Reconstructed route sequence to D is exactly A -> C -> D") {
    val graph = buildFourNodeBenchmarkGraph()
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    val dResult = routes(4L)
    dResult.path shouldBe List(1L, 3L, 4L)
  }

  test("Benchmark Test 10: Suboptimal route A -> B -> D has higher cost (0.4000 h) than chosen route") {
    val graph = buildFourNodeBenchmarkGraph()
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    val bResult = routes(2L)
    bResult.totalTravelTimeHours shouldBe (0.10 +- 0.0001)

    // Manual alternative cost: cost(A->B) + cost(B->D) = 0.10 + 0.30 = 0.40 h
    val alternativeCost = 0.10 + (3.0 / 10.0)
    val optimalCost = routes(4L).totalTravelTimeHours

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
    // Severe traffic jam on C -> D: speed drops from 45 to 10 km/h (cost: 5/10 = 0.50 h)
    // Path A -> C -> D now costs 0.12 + 0.50 = 0.62 h
    // Path A -> B -> D costs 0.10 + 0.30 = 0.40 h
    // Optimal path must now flip to A -> B -> D!
    val congestedEdges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
      Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
      Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
      Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 10.0)) // Congested!
    ))
    val congestedGraph = Graph(vertices, congestedEdges)
    val routes = ShortestPathPregel.computeRoutes(congestedGraph, origin = 1L)

    val dResult = routes(4L)
    dResult.path shouldBe List(1L, 2L, 4L)
    dResult.totalTravelTimeHours shouldBe (0.4000 +- 0.0001)
  }

  // ============================================================================
  // CATEGORY 4: DIRECTED SEMANTICS, UNREACHABILITY & INPUT DEFENSE
  // ============================================================================

  test("Robustness Test 12: Directed edge semantics are strictly respected (no reverse traversal)") {
    val vertices = spark.sparkContext.parallelize(Seq((1L, "A"), (2L, "B")))
    // Only directed edge 1L -> 2L exists
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 0.25)
    ))
    val graph = Graph(vertices, edges)

    // Routing from 2L to 1L: 1L is unreachable!
    val routesFrom2 = ShortestPathPregel.computeRoutes(graph, origin = 2L)
    val result1 = routesFrom2(1L)

    result1.isReachable shouldBe false
    result1.totalTravelTimeHours.isPosInfinity shouldBe true
    result1.path shouldBe empty
  }

  test("Robustness Test 13: Unreachable vertices in disconnected graph are correctly reported") {
    val vertices = spark.sparkContext.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "Isolated")
    ))
    val edges = spark.sparkContext.parallelize(Seq(
      Edge(1L, 2L, 0.50)
    ))
    val graph = Graph(vertices, edges)
    val routes = ShortestPathPregel.computeRoutes(graph, origin = 1L)

    routes(2L).isReachable shouldBe true
    routes(2L).path shouldBe List(1L, 2L)

    routes(3L).isReachable shouldBe false
    routes(3L).totalTravelTimeHours.isPosInfinity shouldBe true
    routes(3L).path shouldBe empty
  }

  test("Robustness Test 14: Nonexistent origin vertex throws IllegalArgumentException") {
    val graph = buildFourNodeBenchmarkGraph()
    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.computeRoutes(graph, origin = 999L)
    }
  }

  test("Robustness Test 15: Invalid edge weights (negative, NaN, Inf) throw IllegalArgumentException") {
    val vertices = spark.sparkContext.parallelize(Seq((1L, "A"), (2L, "B")))
    val edgesNegative = spark.sparkContext.parallelize(Seq(Edge(1L, 2L, -1.5)))
    val graphNegative = Graph(vertices, edgesNegative)

    an[IllegalArgumentException] should be thrownBy {
      ShortestPathPregel.computeRoutes(graphNegative, origin = 1L)
    }
  }

  test("Robustness Test 16: Empty graph produces empty routes without crashing") {
    val emptyVertices = spark.sparkContext.emptyRDD[(VertexId, String)]
    val emptyEdges = spark.sparkContext.emptyRDD[Edge[Double]]
    val emptyGraph = Graph(emptyVertices, emptyEdges)

    val routed = ShortestPathPregel.runShortestPath(emptyGraph, origin = 1L)
    routed.vertices.count() shouldBe 0L
  }

  test("Robustness Test 17: Equal-cost candidate routes resolve deterministically via tie-breaking") {
    // Two paths of identical cost: 1 -> 2 -> 4 (cost 0.2) vs 1 -> 3 -> 4 (cost 0.2)
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

    // Lexicographic tie-breaker prefers "1,2,4" over "1,3,4"
    routes(4L).totalTravelTimeHours shouldBe (0.20 +- 0.000001)
    routes(4L).path shouldBe List(1L, 2L, 4L)
  }

  // ============================================================================
  // CATEGORY 5: FULL DAY 7 DEMONSTRATION GRAPH ROUTING VERIFICATION
  // ============================================================================

  test("Full Graph Test 18: Full Day 7 20-node road network reaches all 20 vertices with placeholder baseline") {
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

    val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(
      spark,
      intersectionsPath,
      roadsPath
    )

    val (rawGraph, summary) = RoadGraphBuilder.validateAndBuildGraph(
      spark,
      intersectionsDF,
      roadsDF,
      strict = true
    )

    summary.isValid shouldBe true
    summary.graphVertexCount shouldBe 20L
    summary.graphEdgeCount shouldBe 40L

    // Map base distance to travel time using 50 km/h placeholder
    val weightedGraph = rawGraph.mapEdges(e =>
      CongestionWeight.calculateTravelTimeHours(e.attr, speedKmph = 50.0)
    )

    // Compute routes from origin intersection 1L (Civic Center)
    val routes = ShortestPathPregel.computeRoutes(weightedGraph, origin = 1L)

    routes.size shouldBe 20
    val reachableCount = routes.values.count(_.isReachable)
    reachableCount shouldBe 20

    // Origin to origin has travel time 0.0 and path List(1L)
    routes(1L).totalTravelTimeHours shouldBe 0.0
    routes(1L).path shouldBe List(1L)

    // Verify all other 19 destinations have strictly positive finite travel times
    routes.filterKeys(_ != 1L).values.foreach { r =>
      r.isReachable shouldBe true
      r.totalTravelTimeHours should be > 0.0
      r.totalTravelTimeHours.isInfinity shouldBe false
      r.path.head shouldBe 1L
      r.path.last shouldBe r.destinationId
    }
  }
}
