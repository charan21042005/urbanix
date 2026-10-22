// ==============================================================================
// Urbanix — Day 8: GraphX Congestion-Weighted Shortest-Path Routing Engine
// ShortestPathPregel.scala — Distributed Pregel Single-Source Shortest Paths (SSSP)
// ==============================================================================
//
// 1. WHY THIS FILE EXISTS:
// In Day 7, Urbanix built a static property graph of 20 intersections and 40 directed roads.
// Today, we implement an active routing engine that computes the fastest (minimum travel time)
// route from any origin intersection to all reachable intersections in the road network.
//
// 2. CRITICAL WARNING: WHY BUILT-IN ShortestPaths IS BANNED:
// Apache Spark GraphX provides a built-in library called `org.apache.spark.graphx.lib.ShortestPaths`.
// HOWEVER, that built-in library ONLY computes UNWEIGHTED HOP COUNTS (number of edges/turns)!
// It completely ignores all edge attributes (distance, speed, travel time).
// If we used `ShortestPaths`, a 2-hop route through a heavily congested road (10 km/h) would be
// chosen over a 2-hop free-flowing highway (100 km/h) because both have 2 hops!
// Therefore, Urbanix implements genuinely WEIGHTED shortest paths using GraphX's Pregel API.
//
// 3. THE PREGEL PROGRAMMING MODEL (FROM ABSOLUTE SCRATCH):
// Google's Pregel is an iterative, vertex-centric, Bulk-Synchronous Parallel (BSP) framework:
// Think of it as a synchronized "telephone game" across the distributed cluster:
//   - Superstep 0:
//       Origin vertex knows its distance is 0.0 hours. All other vertices start at Infinity.
//       Origin whispers to its immediate outgoing neighbors: "Hey, I can reach you in X hours!"
//   - Superstep 1 (Barrier Synchronization):
//       Each neighbor receives incoming whispers. If two neighbors whisper at once, `mergeMsg`
//       keeps the faster route. `vprog` updates the vertex's memory with the new best time.
//       If a vertex's time improved, it calculates new travel times and whispers to its own neighbors.
//   - Supersteps 2 ... N:
//       Rounds continue until no vertex can offer any neighbor a faster route (convergence / fixed point).
//
// 4. PREGEL'S THREE CORE FUNCTIONS:
//   - vprog (Vertex Program):
//       How an individual vertex updates its current state when new candidate messages arrive.
//   - sendMsg (Message Sender):
//       Executed on every directed edge. If a vertex can offer its neighbor a shorter route
//       through itself (srcDist + edgeWeight < dstDist), it sends a message along that directed edge.
//   - mergeMsg (Message Reducer):
//       If two upstream neighbors propose routes to the same vertex in the same round,
//       mergeMsg compares them and retains the one with the minimum travel time.
//
// 5. UNITS & CONVERGENCE:
//   - Edge weights: Travel time in hours (strictly positive and finite).
//   - Distances: Cumulative travel time in hours.
//   - Bounded iterations: In a graph with |V| vertices, any simple shortest path has at most |V| - 1
//     edges. Thus, Pregel is guaranteed to converge in <= |V| - 1 supersteps (Bellman-Ford theorem).
// ==============================================================================

package urbanix.graphx

import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
 * Represents the routing state maintained on each vertex during and after Pregel execution.
 *
 * @param distance Cumulative travel time from the origin to this vertex in hours.
 *                 Initialized to 0.0 for the origin, and Double.PositiveInfinity for others.
 * @param path The ordered sequence of VertexIds traversed from the origin to this vertex.
 *             e.g., List(1L, 3L, 4L) represents origin 1 -> 3 -> 4.
 */
case class RouteState(
  distance: Double,
  path: List[VertexId]
)

/**
 * Message passed along directed edges from an upstream vertex to a downstream neighbor.
 *
 * @param proposedDistance The candidate cumulative travel time through the sender (src.distance + edge.weight).
 * @param proposedPath The candidate route sequence ending at the destination vertex.
 */
case class RouteMessage(
  proposedDistance: Double,
  proposedPath: List[VertexId]
)

/**
 * Final formatted routing result for a single destination vertex, suitable for serialization and tests.
 *
 * @param destinationId Target intersection ID.
 * @param totalTravelTimeHours Shortest travel time in hours (or Double.PositiveInfinity if unreachable).
 * @param path Ordered list of vertex IDs forming the route.
 * @param isReachable True if the vertex can be reached along directed edges from the origin.
 */
case class RouteResult(
  destinationId: VertexId,
  totalTravelTimeHours: Double,
  path: List[VertexId],
  isReachable: Boolean
) {
  def totalTravelTimeMinutes: Double = {
    if (isReachable) totalTravelTimeHours * 60.0 else Double.PositiveInfinity
  }
}

object ShortestPathPregel {

  /**
   * Initial sentinel message used to trigger Superstep 0 in Pregel.
   * Carries Infinite distance so it never overwrites a valid vertex state.
   */
  val InitialSentinelMessage: RouteMessage = RouteMessage(Double.PositiveInfinity, Nil)

  /**
   * Executes weighted Single-Source Shortest Path (SSSP) on a GraphX property graph using Pregel.
   *
   * @tparam VD Vertex attribute type in the input graph (e.g. String intersection name).
   * @param graph The GraphX graph with numeric Double edge weights (travel time in hours).
   * @param origin The 64-bit Long VertexId of the starting intersection.
   * @param maxIterations Optional iteration limit. Defaults to max(1, |V| - 1).
   * @return A new GraphX graph where each vertex holds a RouteState(distance, path).
   */
  def runShortestPath[VD: ClassTag](
    graph: Graph[VD, Double],
    origin: VertexId,
    maxIterations: Option[Int] = None
  ): Graph[RouteState, Double] = {

    // 1. Validate empty graph
    val vertexCount = graph.vertices.count()
    if (vertexCount == 0L) {
      return graph.mapVertices((_, _) => RouteState(Double.PositiveInfinity, Nil))
    }

    // 2. Validate that the requested origin actually exists in the graph
    val originExists = graph.vertices.filter { case (id, _) => id == origin }.count() > 0
    if (!originExists) {
      throw new IllegalArgumentException(
        s"Origin vertex $origin does not exist in the graph (contains $vertexCount vertices)."
      )
    }

    // 3. Validate that all edge weights are finite and non-negative
    val invalidEdgeCount = graph.edges.filter(e => e.attr.isNaN || e.attr.isInfinity || e.attr < 0.0).count()
    if (invalidEdgeCount > 0L) {
      throw new IllegalArgumentException(
        s"Graph contains $invalidEdgeCount invalid edge weights (NaN, Infinity, or negative). All edge weights must be finite and >= 0.0."
      )
    }

    // 4. Initialize Vertex States:
    // Origin starts at distance 0.0 with path [origin].
    // All other vertices start at distance Infinity with empty path [].
    val initialGraph: Graph[RouteState, Double] = graph.mapVertices { (id, _) =>
      if (id == origin) {
        RouteState(0.0, List(origin))
      } else {
        RouteState(Double.PositiveInfinity, Nil)
      }
    }

    // 5. Determine maximum iteration bound:
    // By the Bellman-Ford theorem, in a graph with V vertices, the longest simple shortest path
    // contains at most V - 1 edges. Setting bound = max(1, V - 1) guarantees fixed-point convergence.
    val iterationLimit = maxIterations.getOrElse(math.max(1, (vertexCount - 1).toInt))

    // 6. Execute GraphX Pregel
    val routedGraph: Graph[RouteState, Double] = initialGraph.pregel(
      initialMsg = InitialSentinelMessage,
      maxIterations = iterationLimit,
      activeDirection = EdgeDirection.Out
    )(
      // ------------------------------------------------------------------------
      // VERTEX PROGRAM (vprog):
      // Runs on each vertex receiving incoming messages.
      // Compares the vertex's current distance with the incoming proposed distance.
      // If the incoming proposal is strictly shorter, update state with the new distance and path.
      // ------------------------------------------------------------------------
      vprog = (vertexId: VertexId, currentState: RouteState, incomingMsg: RouteMessage) => {
        if (incomingMsg.proposedDistance < currentState.distance) {
          RouteState(incomingMsg.proposedDistance, incomingMsg.proposedPath)
        } else {
          currentState
        }
      },

      // ------------------------------------------------------------------------
      // SEND MESSAGE (sendMsg):
      // Runs on every directed edge triplet (src) ---[edge.weight]---> (dst).
      // If the source vertex has a known finite distance, propose a candidate route to dst:
      //   proposedDistance = src.distance + edge.weight
      // STRICT DIRECTED SEMANTICS:
      // Messages are ONLY emitted along directed outgoing edges toward triplet.dstId.
      // We NEVER traverse backwards from dst to src unless an explicit reverse edge exists.
      // ------------------------------------------------------------------------
      sendMsg = (triplet: EdgeTriplet[RouteState, Double]) => {
        val srcDist = triplet.srcAttr.distance
        val edgeWeight = triplet.attr

        if (!srcDist.isInfinity && srcDist >= 0.0) {
          val proposedDist = srcDist + edgeWeight

          // Only propose if the candidate route is strictly faster than destination's current distance
          if (proposedDist < triplet.dstAttr.distance) {
            val candidatePath = triplet.srcAttr.path :+ triplet.dstId
            Iterator((triplet.dstId, RouteMessage(proposedDist, candidatePath)))
          } else {
            Iterator.empty
          }
        } else {
          // Source vertex is still unreachable; cannot propose any routes through it yet
          Iterator.empty
        }
      },

      // ------------------------------------------------------------------------
      // MERGE MESSAGE (mergeMsg):
      // Runs when multiple upstream neighbors propose routes to the same vertex in the same round.
      // Compares candidate proposals and retains the one with the MINIMUM travel time.
      // DETERMINISTIC TIE-BREAKING:
      // If two competing paths offer the exact same travel time, break ties lexicographically
      // by comparing the vertex ID path strings (e.g. "1,2,4" vs "1,3,4").
      // This prevents route flapping across distributed executor partitions.
      // ------------------------------------------------------------------------
      mergeMsg = (msgA: RouteMessage, msgB: RouteMessage) => {
        if (msgA.proposedDistance < msgB.proposedDistance) {
          msgA
        } else if (msgB.proposedDistance < msgA.proposedDistance) {
          msgB
        } else {
          // Deterministic tie-breaking on equal cost
          val pathAStr = msgA.proposedPath.mkString(",")
          val pathBStr = msgB.proposedPath.mkString(",")
          if (pathAStr <= pathBStr) msgA else msgB
        }
      }
    )

    routedGraph
  }

  /**
   * High-level convenience function that executes SSSP and collects the results
   * into a driver-side Map[VertexId, RouteResult].
   *
   * @param graph The GraphX property graph.
   * @param origin Starting intersection ID.
   * @return Map of VertexId -> RouteResult containing travel times, paths, and reachability.
   */
  def computeRoutes[VD: ClassTag](
    graph: Graph[VD, Double],
    origin: VertexId
  ): Map[VertexId, RouteResult] = {
    val routedGraph = runShortestPath(graph, origin)

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
}
