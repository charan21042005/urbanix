// ==============================================================================
// 🌟 URBANIX — MULTI-STAGE GRAPHX ROUTING CONSOLE VERIFICATION APP
// FullGraphRoutingApp.scala — Benchmark Oracles, Dynamic Rerouting & Full City Graph
// ==============================================================================
// Welcome to the FullGraphRoutingApp console application! 👋
//
// ------------------------------------------------------------------------------
// 🎓 WHAT DOES THIS SCALA APPLICATION DO?
// ------------------------------------------------------------------------------
// This standalone executable provides a clear 3-stage demonstration of our routing engine:
//
// 📌 STAGE 1: The Hand-Checked 4-Node Mathematical Benchmark (The Oracle)
//    - Tests an illustrative network with 4 intersections (A, B, C, D).
//    - Route 1 (via B): Shorter distance (7 km) but congested (10 km/h) -> 24.0 min.
//    - Route 2 (via C): Longer distance (11 km) but free-flowing (45-50 km/h) -> 13.87 min!
//    - Proves that Pregel correctly chooses the FASTER route (A -> C -> D),
//      even though it has more kilometers!
//
// 📌 STAGE 2: Dynamic Congestion Injection & Automated Rerouting Proof
//    - Suddenly injects a simulated traffic jam on road C -> D (speed drops from 45 to 10 km/h).
//    - Re-runs Pregel and proves that the optimal path automatically FLIPS to A -> B -> D!
//
// 📌 STAGE 3: Full 20-Intersection, 40-Edge Metropolitan City Graph
//    - Loads real CSV files ('data/intersections.csv' and 'data/roads.csv').
//    - Calculates shortest travel times from Origin Node 1 (Civic Center) to all 20 nodes!
//    - Verifies that 100% of intersections are reachable with zero network partitions.
// ==============================================================================

package urbanix.graphx

// Import Spark SQL session and GraphX components
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

/**
 * Singleton object: An executable console application in Scala.
 * 'def main(args: Array[String]): Unit' is the entrypoint (just like in Java or C++)!
 */
object FullGraphRoutingApp {

  /**
   * Static baseline velocity placeholder (50.0 km/h) used for Day 8 full-network testing
   * before Day 9 live MLlib speed forecast integration.
   */
  val BaselinePlaceholderSpeedKmph: Double = 50.0

  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("  URBANIX DAY 8 — CONGESTION-WEIGHTED PREGEL ROUTING ENGINE")
    println("=" * 80)

    // -------------------------------------------------------------------------
    // ⚙️ INITIALIZE LOCAL SPARK SESSION
    // -------------------------------------------------------------------------
    // 'master("local[*]")' tells Spark to run locally on your computer using all CPU cores.
    val spark = SparkSession.builder()
      .appName("UrbanixDay8RoutingEngine")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // Reduce terminal log spam: only show errors
    spark.sparkContext.setLogLevel("ERROR")

    try {
      // ========================================================================
      // 🧪 STAGE 1: 4-NODE HAND-CHECKED MATHEMATICAL BENCHMARK
      // ========================================================================
      println("\n" + "-" * 80)
      println("  STAGE 1: Section 3.1 Hand-Checked 4-Node Test Subgraph")
      println("-" * 80)

      // Map node IDs to friendly letters: 1=A, 2=B, 3=C, 4=D
      val nodeNames = Map(1L -> "A", 2L -> "B", 3L -> "C", 4L -> "D")

      // 'sparkContext.parallelize' creates a distributed RDD (Resilient Distributed Dataset) from a collection
      val testVertices = spark.sparkContext.parallelize(Seq(
        (1L, "Intersection A"),
        (2L, "Intersection B"),
        (3L, "Intersection C"),
        (4L, "Intersection D")
      ))

      // Define directed roads with travel time costs: distance_km / speed_kmph
      //   A -> B: 4 km / 40 km/h = 0.1000 h (6.00 min)
      //   A -> C: 6 km / 50 km/h = 0.1200 h (7.20 min)
      //   B -> D: 3 km / 10 km/h = 0.3000 h (18.00 min) [HEAVILY CONGESTED]
      //   C -> D: 5 km / 45 km/h = 0.1111 h (6.67 min)  [FREE FLOWING]
      val testEdges = spark.sparkContext.parallelize(Seq(
        Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
        Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
        Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
        Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 45.0))
      ))

      // Assemble into a GraphX Property Graph
      val testGraph = Graph(testVertices, testEdges)

      // Run Pregel SSSP starting from Origin Node 1 (A)
      val routesStage1 = ShortestPathPregel.computeRoutes(testGraph, origin = 1L)

      println("Computed Routes from Origin A (1L):")
      routesStage1.toSeq.sortBy(_._1).foreach { case (destId, res) =>
        val name = nodeNames(destId)
        val pathStr = res.path.map(id => nodeNames(id)).mkString(" -> ")
        println(f"  Destination $name ($destId%dL): Travel Time = ${res.totalTravelTimeHours}%.4f h (${res.totalTravelTimeMinutes}%.2f min) | Route: $pathStr")
      }

      // Mathematical Verification:
      // Path A -> C -> D travel time = 0.12 + (5.0 / 45.0) = 0.231111... hours (13.87 min)
      val dResultStage1 = routesStage1(4L)
      val expectedHoursStage1 = 0.12 + (5.0 / 45.0)
      assert(
        math.abs(dResultStage1.totalTravelTimeHours - expectedHoursStage1) < 0.0001,
        s"Stage 1 FAILED: Expected $expectedHoursStage1 hours, got ${dResultStage1.totalTravelTimeHours}"
      )
      assert(
        dResultStage1.path == List(1L, 3L, 4L),
        s"Stage 1 FAILED: Expected path List(1, 3, 4), got ${dResultStage1.path}"
      )
      println(f"[PASS] STAGE 1 ORACLE VERIFIED: Optimum is A -> C -> D with ${dResultStage1.totalTravelTimeHours}%.4f h (13.87 min).")
      println("       Alternative A -> B -> D costs 0.4000 h (24.00 min) — properly penalized due to congestion!")

      // ========================================================================
      // 🔄 STAGE 2: DYNAMIC CONGESTION INJECTION & AUTOMATIC REROUTING PROOF
      // ========================================================================
      println("\n" + "-" * 80)
      println("  STAGE 2: Dynamic Congestion Injection & Automatic Rerouting Proof")
      println("-" * 80)
      println("Injecting sudden severe gridlock on C -> D (speed drops from 45 km/h to 10 km/h)...")

      // Under new condition:
      //   C -> D becomes 5 km / 10 km/h = 0.5000 h (30.0 min)
      //   Path A -> C -> D now costs 0.12 + 0.50 = 0.6200 h (37.2 min)
      //   Path A -> B -> D remains 0.10 + 0.30 = 0.4000 h (24.0 min)
      // The Pregel algorithm must automatically detect this and switch the optimal path to A -> B -> D!
      val congestedEdges = spark.sparkContext.parallelize(Seq(
        Edge(1L, 2L, CongestionWeight.calculateTravelTimeHours(4.0, 40.0)),
        Edge(1L, 3L, CongestionWeight.calculateTravelTimeHours(6.0, 50.0)),
        Edge(2L, 4L, CongestionWeight.calculateTravelTimeHours(3.0, 10.0)),
        Edge(3L, 4L, CongestionWeight.calculateTravelTimeHours(5.0, 10.0)) // GRIDLOCK!
      ))

      val congestedGraph = Graph(testVertices, congestedEdges)
      val routesStage2 = ShortestPathPregel.computeRoutes(congestedGraph, origin = 1L)
      val dResultStage2 = routesStage2(4L)

      println("Recomputed Routes after Congestion Surge on C -> D:")
      routesStage2.toSeq.sortBy(_._1).foreach { case (destId, res) =>
        val name = nodeNames(destId)
        val pathStr = res.path.map(id => nodeNames(id)).mkString(" -> ")
        println(f"  Destination $name ($destId%dL): Travel Time = ${res.totalTravelTimeHours}%.4f h (${res.totalTravelTimeMinutes}%.2f min) | Route: $pathStr")
      }

      // Assert that path flipped to A -> B -> D
      val expectedHoursStage2 = 0.4000
      assert(
        math.abs(dResultStage2.totalTravelTimeHours - expectedHoursStage2) < 0.0001,
        s"Stage 2 FAILED: Expected $expectedHoursStage2 hours, got ${dResultStage2.totalTravelTimeHours}"
      )
      assert(
        dResultStage2.path == List(1L, 2L, 4L),
        s"Stage 2 FAILED: Expected path List(1, 2, 4), got ${dResultStage2.path}"
      )
      println(f"[PASS] STAGE 2 REROUTE PROVEN: Optimum automatically flipped from A->C->D to A->B->D (${dResultStage2.totalTravelTimeHours}%.4f h).")

      // ========================================================================
      // 🏙️ STAGE 3: FULL 20-INTERSECTION, 40-EDGE METROPOLITAN ROAD NETWORK
      // ========================================================================
      println("\n" + "-" * 80)
      println("  STAGE 3: Full 20-Intersection, 40-Edge Day 7 Demonstration Road Graph")
      println("-" * 80)

      // Resolve dataset paths dynamically
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

      println(s"Loading verified datasets:")
      println(s"  - Intersections: $intersectionsPath")
      println(s"  - Roads:         $roadsPath")

      // Load CSVs into Spark DataFrames
      val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(
        spark,
        intersectionsPath,
        roadsPath
      )

      // Build and audit the production GraphX property graph
      val (rawGraph, summary) = RoadGraphBuilder.validateAndBuildGraph(
        spark,
        intersectionsDF,
        roadsDF,
        strict = true
      )

      println(s"Day 7 Topology Audit: ${summary.graphVertexCount} vertices, ${summary.graphEdgeCount} edges, valid=${summary.isValid}")

      println(s"\nWeight Strategy: Applying static placeholder baseline speed ($BaselinePlaceholderSpeedKmph km/h)")
      println("                 Formula: travel_time_hours = base_distance_km / 50.0 km/h")
      println("                 Note: Live MLlib prediction integration is explicitly scheduled for Day 9.")

      // Transform edge base distances into travel times in hours
      val weightedGraph = rawGraph.mapEdges(e =>
        CongestionWeight.calculateTravelTimeHours(e.attr, BaselinePlaceholderSpeedKmph)
      )

      val originId = 1L // Civic Center / Downtown Core
      println(s"\nExecuting Pregel SSSP from Origin Intersection 1L (Civic Center)...")
      val fullRoutes = ShortestPathPregel.computeRoutes(weightedGraph, originId)

      // Print clean routing table
      println(f"\n${"ID"}%-4s | ${"Travel Time"}%-18s | ${"Reachable"}%-10s | ${"Route Sequence"}")
      println("-" * 80)
      fullRoutes.toSeq.sortBy(_._1).foreach { case (destId, res) =>
        val timeStr = if (res.isReachable) f"${res.totalTravelTimeHours}%.4f h (${res.totalTravelTimeMinutes}%.1f m)" else "UNREACHABLE"
        val routeStr = if (res.isReachable) res.path.mkString(" -> ") else "NONE"
        println(f"$destId%-4d | $timeStr%-18s | ${res.isReachable}%-10s | $routeStr")
      }

      // Assert all 20 nodes are reached
      val reachableCount = fullRoutes.values.count(_.isReachable)
      val unreachableCount = fullRoutes.values.count(!_.isReachable)
      println("-" * 80)
      println(s"Summary: $reachableCount / 20 reachable vertices from origin 1L ($unreachableCount unreachable).")
      assert(reachableCount == 20, s"Expected 20 reachable vertices, found $reachableCount")
      println("[PASS] STAGE 3 FULL GRAPH ROUTING VERIFIED: All 20 intersections reachable.")

      println("\n" + "=" * 80)
      println("  [VERDICT] ALL 3 ROUTING STAGES COMPLETED AND VERIFIED SUCCESSFULLY!")
      println("=" * 80)

    } finally {
      // Clean up Spark resources
      println("\nShutting down SparkSession cleanly...")
      spark.stop()
      println("[OK] SparkSession stopped.")
    }
  }
}
