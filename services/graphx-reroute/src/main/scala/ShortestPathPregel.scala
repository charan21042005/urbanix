// ==============================================================================
// 🌟 URBANIX — DISTRIBUTED GRAPHX PREGEL SHORTEST-PATH ROUTING ENGINE
// ShortestPathPregel.scala — Bulk-Synchronous Parallel (BSP) Weighted SSSP
// ==============================================================================
// Welcome to the ShortestPathPregel engine! 👋
//
// ------------------------------------------------------------------------------
// 🎓 1. WHAT IS PREGEL? (THE DISTRIBUTED TELEPHONE GAME)
// ------------------------------------------------------------------------------
// Google's "Pregel" is an algorithm model designed for massive computer clusters.
// Instead of one computer tracing paths like Dijkstra's algorithm in a loop,
// every intersection (Vertex) acts like an independent thinker!
//
// Think of it as a synchronized "telephone game" across computers:
//   - Superstep 0:
//       The Origin vertex knows its distance is 0.0 hours.
//       All other vertices start with distance = Infinity.
//       Origin sends a message to its outgoing neighbors: "Hey, I can reach you in X hours!"
//   - Superstep 1:
//       Each neighbor receives incoming messages.
//       If two neighbors message the same intersection, 'mergeMsg' picks the faster one.
//       Then 'vprog' updates the intersection's memory with this new best time.
//       If an intersection's time improved, it calculates new travel times and messages its own neighbors!
//   - Supersteps 2 ... N:
//       The process repeats across the cluster until no intersection can find a faster route!
//       This state is called "Convergence" (or fixed point).
//
// ------------------------------------------------------------------------------
// ⚠️ 2. WHY APACHE SPARK'S BUILT-IN ShortestPaths IS NOT USED:
// ------------------------------------------------------------------------------
// Spark GraphX comes with a built-in library called 'org.apache.spark.graphx.lib.ShortestPaths'.
// HOWEVER, that built-in tool ONLY counts UNWEIGHTED HOPS (number of road intersections)!
// It completely ignores road lengths and traffic speeds!
// Under built-in ShortestPaths, a 2-hop road in heavy gridlock (10 km/h) would be treated
// as identical to a 2-hop free-flowing freeway (100 km/h).
// That's why Urbanix implements a genuinely WEIGHTED Pregel shortest path algorithm here!
//
// ------------------------------------------------------------------------------
// 🧩 3. PREGEL'S THREE CORE FUNCTIONS:
// ------------------------------------------------------------------------------
//   - vprog (Vertex Program):
//       How a vertex updates its internal state when a new candidate route message arrives.
//   - sendMsg (Message Sender):
//       Executed on every directed edge. If a vertex can offer its neighbor a shorter path
//       through itself (srcDist + edgeWeight < dstDist), it sends a message along that road.
//   - mergeMsg (Message Reducer):
//       If multiple roads lead into the same intersection in the same round,
//       mergeMsg compares them and keeps only the route with the minimum travel time!
// ==============================================================================

package urbanix.graphx

// Import Apache Spark GraphX graph processing classes
import org.apache.spark.graphx._

// 'ClassTag' is a Scala feature that preserves type information at runtime for generic types [VD]
import scala.reflect.ClassTag

/**
 * -----------------------------------------------------------------------------
 * 📦 CASE CLASS: RouteState
 * -----------------------------------------------------------------------------
 * Represents the memory state stored on EACH vertex (intersection) during routing.
 * In Scala, a 'case class' is a lightweight immutable data container with built-in
 * getters, equals(), hashCode(), and toString() methods.
 *
 * @param distance Cumulative travel time from the origin to this vertex in hours.
 *                 (0.0 for Origin Node 1, and PositiveInfinity for all other nodes initially).
 * @param path     The ordered sequence of 64-bit Long VertexIds traversed from origin to here.
 *                 e.g. List(1L, 2L, 14L, 16L, 15L).
 */
case class RouteState(
  distance: Double,
  path: List[VertexId]
)

/**
 * -----------------------------------------------------------------------------
 * 📦 CASE CLASS: RouteMessage
 * -----------------------------------------------------------------------------
 * The data packet passed along directed road edges from an upstream node to its neighbor.
 *
 * @param proposedDistance The candidate travel time through the sender (src.distance + edge.weight).
 * @param proposedPath     The candidate sequence of intersections leading to the destination.
 */
case class RouteMessage(
  proposedDistance: Double,
  proposedPath: List[VertexId]
)

/**
 * -----------------------------------------------------------------------------
 * 📦 CASE CLASS: RouteResult
 * -----------------------------------------------------------------------------
 * Clean, user-friendly output record representing the final routing decision for a destination.
 *
 * @param destinationId        The target intersection ID.
 * @param totalTravelTimeHours Shortest travel time in hours (or PositiveInfinity if unreachable).
 * @param path                 Ordered list of intersection IDs forming the fastest path.
 * @param isReachable          True if a valid directed road path exists from origin to destination.
 */
case class RouteResult(
  destinationId: VertexId,
  totalTravelTimeHours: Double,
  path: List[VertexId],
  isReachable: Boolean
) {
  /**
   * Helper method: Converts hours into human-readable minutes (e.g. 0.317 hours -> 19.02 minutes).
   */
  def totalTravelTimeMinutes: Double = {
    if (isReachable) totalTravelTimeHours * 60.0 else Double.PositiveInfinity
  }
}

/**
 * Singleton object containing the Pregel Single-Source Shortest Path (SSSP) algorithms.
 */
object ShortestPathPregel {

  /**
   * Initial sentinel message used to wake up all vertices in Superstep 0.
   * Carries Infinite distance so it never accidentally overwrites valid vertex states.
   * 'Nil' is Scala's empty list: List().
   */
  val InitialSentinelMessage: RouteMessage = RouteMessage(Double.PositiveInfinity, Nil)

  /**
   * Executes weighted Single-Source Shortest Path (SSSP) across a distributed GraphX graph.
   *
   * @tparam VD       The type of vertex data (e.g. String intersection names).
   * @param graph     The GraphX graph whose edge attributes are Double travel times in hours.
   * @param origin    The 64-bit Long VertexId of the dispatch starting point (e.g. Node 1L).
   * @param maxIterations Optional limit on Pregel rounds. Defaults to (V - 1) iterations.
   * @return          A new GraphX graph where every vertex holds its optimal RouteState(distance, path).
   */
  def runShortestPath[VD: ClassTag](
    graph: Graph[VD, Double],
    origin: VertexId,
    maxIterations: Option[Int] = None
  ): Graph[RouteState, Double] = {

    // -------------------------------------------------------------------------
    // 🔍 STEP 1: DEFENSIVE GRAPH VALIDATION
    // -------------------------------------------------------------------------
    val vertexCount = graph.vertices.count()
    if (vertexCount == 0L) {
      // If graph has no vertices, return an empty graph immediately without error
      return graph.mapVertices((_, _) => RouteState(Double.PositiveInfinity, Nil))
    }

    // Verify that the requested origin intersection actually exists in the graph!
    val originExists = graph.vertices.filter { case (id, _) => id == origin }.count() > 0
    if (!originExists) {
      throw new IllegalArgumentException(
        s"Origin vertex $origin does not exist in the graph (contains $vertexCount vertices)."
      )
    }

    // Verify all road edge weights are valid positive numbers (no negative or NaN distances)
    val invalidEdgeCount = graph.edges.filter(e => e.attr.isNaN || e.attr.isInfinity || e.attr < 0.0).count()
    if (invalidEdgeCount > 0L) {
      throw new IllegalArgumentException(
        s"Graph contains $invalidEdgeCount invalid edge weights (NaN, Infinity, or negative). All edge weights must be finite and >= 0.0."
      )
    }

    // -------------------------------------------------------------------------
    // 🏁 STEP 2: INITIALIZE VERTEX STATES
    // -------------------------------------------------------------------------
    // Origin starts at distance 0.0 with path [origin].
    // Every other intersection starts at distance Infinity with an empty path [].
    val initialGraph: Graph[RouteState, Double] = graph.mapVertices { (id, _) =>
      if (id == origin) {
        RouteState(0.0, List(origin))
      } else {
        RouteState(Double.PositiveInfinity, Nil)
      }
    }

    // -------------------------------------------------------------------------
    // 📐 STEP 3: BOUND MAXIMUM ITERATIONS (BELLMAN-FORD THEOREM)
    // -------------------------------------------------------------------------
    // Mathematical Theorem: In any graph with V vertices, the longest simple path
    // (a path with no repeated vertices) can have at most (V - 1) road segments!
    // Therefore, Pregel is mathematically guaranteed to find the true shortest path
    // in at most (V - 1) rounds!
    maxIterations.foreach { iters =>
      if (iters <= 0) {
        throw new IllegalArgumentException(s"maxIterations must be strictly positive (> 0), observed: $iters")
      }
    }
    val iterationLimit = maxIterations.getOrElse(math.max(1, (vertexCount - 1).toInt))

    // -------------------------------------------------------------------------
    // 🚀 STEP 4: EXECUTE DISTRIBUTED PREGEL COMPUTATION
    // -------------------------------------------------------------------------
    val routedGraph: Graph[RouteState, Double] = initialGraph.pregel(
      initialMsg = InitialSentinelMessage,
      maxIterations = iterationLimit,
      activeDirection = EdgeDirection.Out // Only send messages forward along directed edges!
    )(
      // -----------------------------------------------------------------------
      // A) VERTEX PROGRAM (vprog):
      // Runs on each vertex when incoming candidate messages arrive.
      // If the incoming proposed travel time is faster than the current best time,
      // update the vertex's state with the new distance and path sequence!
      // -----------------------------------------------------------------------
      vprog = (vertexId: VertexId, currentState: RouteState, incomingMsg: RouteMessage) => {
        if (incomingMsg.proposedDistance < currentState.distance) {
          RouteState(incomingMsg.proposedDistance, incomingMsg.proposedPath)
        } else {
          currentState // Keep existing best route
        }
      },

      // -----------------------------------------------------------------------
      // B) SEND MESSAGE (sendMsg):
      // Runs across every directed road edge triplet: (source) ---[weight]---> (destination).
      // If source vertex has a known finite route, propose a path through itself to neighbor:
      //   proposedTime = source.distance + road.travel_time
      // -----------------------------------------------------------------------
      sendMsg = (triplet: EdgeTriplet[RouteState, Double]) => {
        val srcDist = triplet.srcAttr.distance
        val edgeWeight = triplet.attr

        // If source has been reached and has a valid distance
        if (!srcDist.isInfinity && srcDist >= 0.0) {
          val proposedDist = srcDist + edgeWeight

          // Only propose if this route is faster than the neighbor's current known time!
          if (proposedDist < triplet.dstAttr.distance) {
            // Append destination ID to the path: candidatePath = src.path + dstId
            val candidatePath = triplet.srcAttr.path :+ triplet.dstId
            Iterator((triplet.dstId, RouteMessage(proposedDist, candidatePath)))
          } else {
            Iterator.empty // No message needed; cannot improve neighbor's route
          }
        } else {
          Iterator.empty // Source hasn't been reached yet
        }
      },

      // -----------------------------------------------------------------------
      // C) MERGE MESSAGE (mergeMsg):
      // Runs if multiple upstream roads propose routes to the same intersection
      // in the EXACT same round. Keeps the candidate proposal with the MINIMUM time!
      // Deterministic tie-breaking: If two routes have the exact same travel time,
      // compare their path strings lexicographically to prevent route flickering.
      // -----------------------------------------------------------------------
      mergeMsg = (msgA: RouteMessage, msgB: RouteMessage) => {
        if (msgA.proposedDistance < msgB.proposedDistance) {
          msgA
        } else if (msgB.proposedDistance < msgA.proposedDistance) {
          msgB
        } else {
          // Tie-breaking on identical costs
          val pathAStr = msgA.proposedPath.mkString(",")
          val pathBStr = msgB.proposedPath.mkString(",")
          if (pathAStr <= pathBStr) msgA else msgB
        }
      }
    )

    // -------------------------------------------------------------------------
    // 🛡️ STEP 5: VERIFY COMPLETE FIXED-POINT CONVERGENCE
    // -------------------------------------------------------------------------
    // Guard against artificial limits: If the user provided a maxIterations limit
    // that was too small, check if any edge could still be improved.
    val unrelaxedEdgesCount = routedGraph.triplets.filter { triplet =>
      val srcDist = triplet.srcAttr.distance
      val dstDist = triplet.dstAttr.distance
      val edgeWeight = triplet.attr
      !srcDist.isInfinity && srcDist >= 0.0 && (srcDist + edgeWeight < dstDist - 1e-9)
    }.count()

    if (unrelaxedEdgesCount > 0L) {
      throw new IllegalStateException(
        s"Pregel SSSP did not converge within the configured iteration limit ($iterationLimit iterations). " +
        s"Found $unrelaxedEdgesCount directed edge(s) where further travel-time improvements remain possible."
      )
    }

    routedGraph
  }

  /**
   * High-level convenience method:
   * Executes SSSP and collects the results into a driver-side Scala Map[VertexId, RouteResult].
   *
   * @param graph  The GraphX property graph.
   * @param origin Starting intersection ID.
   * @return       Map of destination ID -> RouteResult.
   */
  def computeRoutes[VD: ClassTag](
    graph: Graph[VD, Double],
    origin: VertexId,
    maxIterations: Option[Int] = None
  ): Map[VertexId, RouteResult] = {
    val routedGraph = runShortestPath(graph, origin, maxIterations)

    // Collect vertex states from distributed executor nodes to the driver computer
    routedGraph.vertices.collect().map { case (destId, state) =>
      val isReachable = !state.distance.isInfinity && state.path.nonEmpty
      destId -> RouteResult(
        destinationId = destId,
        totalTravelTimeHours = state.distance,
        path = if (isReachable) state.path else Nil,
        isReachable = isReachable
      )
    }.toMap
  }

  /**
   * Compatibility alias for runShortestPath.
   */
  def computeShortestPaths[VD: ClassTag](
    graph: Graph[VD, Double],
    originId: VertexId,
    maxIterations: Option[Int] = None
  ): Graph[RouteState, Double] = runShortestPath(graph, originId, maxIterations)

  /**
   * Helper function: Retrieves the shortest path to a single specific destination intersection.
   */
  def tracePath(
    resultGraph: Graph[RouteState, Double],
    destinationId: VertexId
  ): Option[RouteState] = {
    resultGraph.vertices
      .filter { case (id, _) => id == destinationId }
      .map { case (_, state) => state }
      .collect()
      .headOption
  }
}
