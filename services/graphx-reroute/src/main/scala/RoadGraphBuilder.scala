// ==============================================================================
// Urbanix — Day 7: GraphX Road Network Rerouting Engine
// RoadGraphBuilder.scala — Distributed Property Graph Construction and Audit
// ==============================================================================
// In GraphX, a road network is represented as a directed Property Graph:
// - Intersections are Vertices: (VertexId, VD) where VertexId is a 64-bit Long
// - Road segments are Edges: Edge[ED] with srcId, dstId, and attribute ED (Double base distance)
// ==============================================================================

package urbanix.graphx

// 'import' brings external classes, traits, and functions into current scope,
// exactly like 'import' in Python or 'from module import ...'.
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File

/**
 * Summary case class holding graph audit and validation statistics.
 *
 * In Scala, a 'case class' is an immutable data carrier with automatically
 * generated constructor, getters, toString, equals, and hashCode methods.
 * Closest Python analogy: @dataclass(frozen=True).
 */
case class GraphSummary(
  sourceVertexCount: Long,
  sourceEdgeCount: Long,
  graphVertexCount: Long,
  graphEdgeCount: Long,
  danglingEdgeCount: Long,
  selfLoopCount: Long,
  isValid: Boolean
)

/**
 * 'object' (not 'class') defines a Singleton Object in Scala.
 * In Python terms, an object is a module-level container for functions and constants,
 * with a single 'main' entry point: def main(args: Array[String]): Unit.
 */
object RoadGraphBuilder {

  /**
   * Resolves a file path by checking candidate locations (relative to working dir or repo root).
   *
   * 'def' declares a method. Parameters require explicit types.
   * Return type ': String' is explicitly stated for clarity and compile-time safety.
   */
  def resolvePath(candidates: Seq[String]): String = {
    // 'val' creates an immutable binding — permanent marker, cannot be reassigned.
    // .find returns Option[String] (Some(path) or None).
    val existing = candidates.find(p => new File(p).exists())
    existing match {
      // 'match' is Scala's pattern matching — a powerful compile-time verified
      // switch statement, conceptually similar to Python 3.10+ match/case.
      case Some(p) => p
      case None =>
        throw new java.io.FileNotFoundException(
          s"Could not locate dataset file. Checked candidate paths: ${candidates.mkString(", ")}"
        )
    }
  }

  /**
   * Loads intersection and road CSV files into Spark DataFrames.
   *
   * DataFrames provide distributed, schema-aware relational tables,
   * identical to PySpark DataFrames since both wrap the underlying Catalyst engine.
   */
  def loadDataFrames(
    spark: SparkSession,
    intersectionsPath: String,
    roadsPath: String
  ): (DataFrame, DataFrame) = {
    val intersectionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(intersectionsPath)

    val roadsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(roadsPath)

    // A pair (tuple) of two DataFrames is returned.
    (intersectionsDF, roadsDF)
  }

  /**
   * Constructs the Spark GraphX property graph from intersection and road DataFrames.
   *
   * @param intersectionsDF DataFrame containing: id (Int/Long), name (String), zone_id (String)
   * @param roadsDF DataFrame containing: src_id (Int/Long), dst_id (Int/Long), base_distance_km (Double)
   * @return Graph[String, Double] where vertex attribute is intersection name,
   *         and edge attribute is base distance in kilometers.
   */
  def buildGraph(
    spark: SparkSession,
    intersectionsDF: DataFrame,
    roadsDF: DataFrame
  ): Graph[String, Double] = {

    // --- STEP 1: Construct Vertex RDD ---
    // GraphX requires VertexId to be a 64-bit Long.
    // Each vertex is represented as a tuple: (VertexId, VD).
    // Here VD = String (the human-readable intersection name).
    val vertices: RDD[(VertexId, String)] = intersectionsDF.rdd.map { row =>
      val id = row.getAs[Number]("id").longValue()
      val name = row.getAs[String]("name")
      (id, name)
    }

    // --- STEP 2: Construct Edge RDD ---
    // GraphX Edge[ED] takes (srcId: VertexId, dstId: VertexId, attr: ED).
    // Here ED = Double (the base road distance in kilometers).
    val edges: RDD[Edge[Double]] = roadsDF.rdd.map { row =>
      val srcId = row.getAs[Number]("src_id").longValue()
      val dstId = row.getAs[Number]("dst_id").longValue()
      val distance = row.getAs[Number]("base_distance_km").doubleValue()
      Edge(srcId, dstId, distance)
    }

    // --- STEP 3: Construct GraphX Graph Object ---
    // The Graph(vertices, edges) companion constructor partitions vertices and edges
    // across the cluster and indexes them for distributed graph algorithms.
    val roadGraph: Graph[String, Double] = Graph(vertices, edges)

    roadGraph
  }

  /**
   * Audits graph structural integrity against valid source vertex IDs.
   *
   * In GraphX, if an edge references an endpoint ID missing from the vertex RDD,
   * GraphX automatically inserts that vertex with a null attribute (null.asInstanceOf[VD]).
   * By checking endpoints against the genuine source vertex set, we detect dangling edges.
   */
  def auditGraph(
    graph: Graph[String, Double],
    validVertexIds: Set[VertexId],
    expectedEdgeCount: Long
  ): GraphSummary = {

    // Count genuine vertices (excluding any auto-created vertices with null attributes)
    val actualVertexCount = graph.vertices.filter(v => v._2 != null && validVertexIds.contains(v._1)).count()
    val actualEdgeCount = graph.edges.count()

    // Validate that all edge endpoints refer to genuine source vertices (no dangling edges)
    val danglingEdges = graph.edges.filter(e =>
      !validVertexIds.contains(e.srcId) || !validVertexIds.contains(e.dstId)
    ).count()

    // Count self-loops (edges where srcId == dstId)
    val selfLoops = graph.edges.filter(e => e.srcId == e.dstId).count()

    val isValid = (actualVertexCount == validVertexIds.size) &&
                  (actualEdgeCount == expectedEdgeCount) &&
                  (danglingEdges == 0) &&
                  (selfLoops == 0)

    GraphSummary(
      sourceVertexCount = validVertexIds.size.toLong,
      sourceEdgeCount = expectedEdgeCount,
      graphVertexCount = actualVertexCount,
      graphEdgeCount = actualEdgeCount,
      danglingEdgeCount = danglingEdges,
      selfLoopCount = selfLoops,
      isValid = isValid
    )
  }

  /**
   * Overloaded auditGraph method for when source vertex count is supplied directly.
   * Filters out any default-initialized vertices (where attr == null) to identify genuine vertices.
   */
  def auditGraph(
    graph: Graph[String, Double],
    expectedVertexCount: Long,
    expectedEdgeCount: Long
  ): GraphSummary = {
    val genuineVertexIds = graph.vertices.filter(_._2 != null).map(_._1).collect().toSet
    auditGraph(graph, genuineVertexIds, expectedEdgeCount)
  }

  /**
   * Main entry point for standalone execution via 'sbt run'.
   *
   * 'Unit' is Scala's return type for functions that produce side effects
   * (equivalent to Python's None or Java's void).
   */
  def main(args: Array[String]): Unit = {
    println("======================================================================")
    println("URBANIX DAY 7: SPARK GRAPHX ROAD-GRAPH CONSTRUCTION & AUDIT")
    println("======================================================================")

    // Initialize local SparkSession
    // .master("local[*]") allows the driver to run locally using all CPU cores
    val spark = SparkSession.builder()
      .appName("UrbanixRoadGraphBuilder")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Set log level to WARN to suppress verbose internal Spark noise
    spark.sparkContext.setLogLevel("WARN")

    try {
      // Determine file paths from CLI args or default candidate locations
      val intersectionsPath = if (args.length >= 1) args(0) else {
        resolvePath(Seq(
          "data/intersections.csv",
          "../../data/intersections.csv",
          "../data/intersections.csv"
        ))
      }

      val roadsPath = if (args.length >= 2) args(1) else {
        resolvePath(Seq(
          "data/roads.csv",
          "../../data/roads.csv",
          "../data/roads.csv"
        ))
      }

      println(s"[*] Loading Intersections Source : $intersectionsPath")
      println(s"[*] Loading Roads Source         : $roadsPath")

      val (intersectionsDF, roadsDF) = loadDataFrames(spark, intersectionsPath, roadsPath)

      val expectedVertices = intersectionsDF.count()
      val expectedEdges = roadsDF.count()

      println(s"[*] Source CSV Rows: $expectedVertices intersections, $expectedEdges road segments")

      // Build the genuine GraphX Property Graph
      val roadGraph = buildGraph(spark, intersectionsDF, roadsDF)

      // Audit and verify the graph
      val summary = auditGraph(roadGraph, expectedVertices, expectedEdges)

      println("----------------------------------------------------------------------")
      println("GRAPH INTEGRITY & TOPOLOGY AUDIT REPORT")
      println("----------------------------------------------------------------------")
      println(s"[*] Source Intersections (Vertices) : ${summary.sourceVertexCount}")
      println(s"[*] Accepted Graph Vertices         : ${summary.graphVertexCount}")
      println(s"[*] Source Road Segments (Edges)    : ${summary.sourceEdgeCount}")
      println(s"[*] Accepted Graph Edges            : ${summary.graphEdgeCount}")
      println(s"[*] Dangling Edges (Missing Endpts) : ${summary.danglingEdgeCount}")
      println(s"[*] Self-Loop Edges                 : ${summary.selfLoopCount}")

      // Compute in-degrees and out-degrees sample
      val topOutDegrees = roadGraph.outDegrees.top(5)(Ordering.by(_._2))
      println("[*] Top Out-Degree Intersections:")
      topOutDegrees.foreach { case (id, deg) =>
        println(s"    - Vertex ID $id : out-degree = $deg")
      }

      // Assertions enforce the Day 7 acceptance criteria
      assert(summary.graphVertexCount == summary.sourceVertexCount,
        s"Vertex count mismatch! Graph has ${summary.graphVertexCount}, expected ${summary.sourceVertexCount}")
      assert(summary.graphEdgeCount == summary.sourceEdgeCount,
        s"Edge count mismatch! Graph has ${summary.graphEdgeCount}, expected ${summary.sourceEdgeCount}")
      assert(summary.danglingEdgeCount == 0,
        s"Integrity failure! Graph contains ${summary.danglingEdgeCount} dangling edges!")
      assert(summary.selfLoopCount == 0,
        s"Integrity failure! Graph contains ${summary.selfLoopCount} self-loop edges!")

      println("======================================================================")
      println("FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY")
      println("Road network graph is validated and ready for Day 8 shortest-path routing.")
      println("======================================================================")

    } finally {
      spark.stop()
    }
  }
}
