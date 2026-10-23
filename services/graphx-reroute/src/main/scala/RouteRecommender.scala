// ==============================================================================
// Urbanix — Day 9: End-to-End Forecast-to-Reroute Integration Engine
// RouteRecommender.scala — Closed-Loop Shared Parquet Inference Consumer & SSSP
// ==============================================================================
//
// 1. WHY SHARED PARQUET IS THE INTEGRATION CONTRACT (PYTHON <-> SCALA):
// In a modern polyglot data architecture, different components excel at different tasks:
//   - Python / PySpark: Excels at streaming ingestion, feature engineering, and MLlib model training.
//   - Scala / Spark GraphX: Excels at distributed graph algorithms, memory-efficient Pregel BSP iterations,
//     and strongly typed property graph processing.
//
// Attempting to bridge Python and Scala via direct runtime interop (e.g., Py4J callbacks, custom sockets,
// or JNI wrappers) introduces tight coupling, JVM-Python process synchronization bugs, serialization overhead,
// and brittle failure domains.
//
// Instead, Urbanix employs SHARED STORAGE INTEGRATION via Apache Parquet:
//   - Both systems agree on a durable directory (`data/feature_table/traffic`) and a strongly typed schema.
//   - Python appends velocity predictions; Scala reads them on demand.
//   - Neither process needs to know the internal code, programming language, or deployment lifecycle of the other.
//   - Parquet guarantees columnar compression (Snappy), metadata tracking, and schema enforcement across languages.
//
// 2. WHY POLLING WAS CHOSEN OVER COMPLEX EVENT-DRIVEN MESSAGING:
// In this production-grade capstone, we implement a periodic polling loop (default: 60 seconds).
//   - Polling is resilient: If the streaming inference job crashes or restarts, the routing engine
//     gracefully uses cached or fallback data without dropping connections.
//   - Predictable resource utilization: Graph computations run at controlled, regular intervals rather
//     than thrashing the cluster with recomputations on every single micro-batch.
//   - Simplicity and maintainability: It achieves high reliability without introducing another layer of
//     distributed brokers (e.g. separate Kafka CDC topics or filesystem notification sidecars).
//
// 3. WHY OVERWRITE MODE IS CORRECT FOR ROUTE RECOMMENDATIONS:
// In Day 3 and Day 6, the streaming pipeline used APPEND mode because historical sensor readings and
// windowed predictions represent permanent, immutable time-series facts.
// In contrast, `data/recommended_routes/` represents a CURRENT OPERATIONAL SNAPSHOT:
//   - A commuter or the Day 10 geospatial dashboard only needs the FASTEST CURRENT ROUTE right now.
//   - Appending routes would require downstream dashboard consumers to execute expensive window aggregations
//     to filter out yesterday's stale paths.
//   - Overwriting guarantees that reading `data/recommended_routes/` is an O(1) query returning solely the
//     latest, freshest optimal routing decision.
//
// 4. WHY DESTINATION INTERSECTION ZONE GOVERNS EDGE CONGESTION:
// Road edges in GraphX connect physical intersections (src_id -> dst_id).
// When traffic enters an edge toward intersection v, congestion bottlenecks occur predominantly at the
// approach and traffic signal of the destination intersection v (queuing, turn lanes, crossing conflicts).
// Therefore, mapping each directed road edge to its DESTINATION intersection's zone velocity models realistic
// urban corridor dynamics accurately.
// ==============================================================================

package urbanix.graphx

// --- Apache Spark SQL Core & DataFrame Imports ---
// SparkSession is the central unified connection to all Spark functionality (DataFrames, Datasets, SQL).
// DataFrame is Spark's distributed table abstraction of rows and columns.
// Row represents an untyped individual tuple/record inside a DataFrame.
import org.apache.spark.sql.{SparkSession, DataFrame, Row}

// Functions contains built-in column manipulation methods like col(), lit(), trim(), length(), isnan().
import org.apache.spark.sql.functions._

// Window allows calculating metrics across groups of rows (partitionBy) with specific ordering (orderBy).
import org.apache.spark.sql.expressions.Window

// Types contains Spark SQL data types (TimestampType, LongType, DoubleType, StringType).
import org.apache.spark.sql.types._

// --- Apache Spark GraphX Imports ---
// Graph is GraphX's core directed multigraph data structure with vertices and edges.
// VertexId is a type alias for Long (64-bit integer), uniquely identifying each intersection node.
import org.apache.spark.graphx._

// RDD (Resilient Distributed Dataset) is Spark's fundamental immutable distributed collection.
import org.apache.spark.rdd.RDD

// --- Java Standard Library Utility Imports ---
// File provides file system path checks and file existence verification.
import java.io.File
// Timestamp represents SQL timestamps down to millisecond/microsecond precision.
import java.sql.Timestamp
// Instant represents an instantaneous point on the UTC timeline.
import java.time.Instant

/**
 * Summary of zone predictions extracted from the shared feature table.
 *
 * In Scala, a `case class` is an immutable data transfer object that automatically generates
 * equals, hashCode, toString, and pattern matching support with zero boilerplate.
 *
 * @param totalRecordsRead Total prediction records scanned in the Parquet store.
 * @param distinctZonesCount Number of unique zones present with valid forecasts.
 * @param zoneSpeeds Map of zone_id -> latest predicted speed in km/h.
 * @param latestPredictionTime Most recent prediction or window timestamp observed.
 * @param isFromFallback True if no valid predictions could be loaded and default speeds were applied.
 */
case class PredictionExtractionSummary(
  totalRecordsRead: Long,
  distinctZonesCount: Int,
  zoneSpeeds: Map[String, Double],
  latestPredictionTime: Option[Timestamp],
  isFromFallback: Boolean
)

/**
 * Audit metrics for the edge reweighting process.
 * Provides complete operational observability into how many edges used real ML predictions vs default speeds.
 *
 * @param totalEdges Total directed road edges reweighted.
 * @param predictedEdgesCount Edges weighted using genuine MLlib model predictions.
 * @param fallbackEdgesCount Edges weighted using the documented free-flow fallback.
 * @param averageSpeedKmph Average effective speed across all road edges in km/h.
 * @param minTravelTimeHours Minimum edge travel time in hours.
 * @param maxTravelTimeHours Maximum edge travel time in hours.
 */
case class EdgeMappingSummary(
  totalEdges: Long,
  predictedEdgesCount: Long,
  fallbackEdgesCount: Long,
  averageSpeedKmph: Double,
  minTravelTimeHours: Double,
  maxTravelTimeHours: Double
)

/**
 * Result record formatted for the Day 10 dashboard-readable route table.
 * Written directly to Parquet so FastAPI / Leaflet.js can display recommended detours instantly.
 *
 * @param origin_id Starting intersection ID.
 * @param destination_id Target intersection ID.
 * @param path Ordered array of intersection IDs forming the shortest path.
 * @param path_formatted Human-readable string representation (e.g. "1 -> 3 -> 5").
 * @param total_travel_time_hours Optimal travel time in hours.
 * @param total_travel_time_minutes Optimal travel time converted to minutes.
 * @param is_reachable True if a directed path exists from origin to destination.
 * @param computed_at Timestamp when this recommendation was computed.
 * @param prediction_zones_used Count of active prediction zones applied.
 * @param fallback_applied True if any road along the network utilized fallback speed.
 */
case class RecommendedRouteRow(
  origin_id: Long,
  destination_id: Long,
  path: Array[Long],
  path_formatted: String,
  total_travel_time_hours: Double,
  total_travel_time_minutes: Double,
  is_reachable: Boolean,
  computed_at: Timestamp,
  prediction_zones_used: Int,
  fallback_applied: Boolean
)

/**
 * Configuration options for RouteRecommender.
 * Default arguments allow instantiating with zero parameters (`RecommenderConfig()`),
 * while CLI arguments can selectively override fields using Scala's `.copy()` method.
 */
case class RecommenderConfig(
  featureTablePath: String = "data/feature_table/traffic",
  intersectionsPath: String = "data/intersections.csv",
  roadsPath: String = "data/roads.csv",
  outputPath: String = "data/recommended_routes",
  originId: Long = 1L,
  pollIntervalSeconds: Int = 60,
  runOnce: Boolean = false,
  freeFlowFallbackKmph: Double = 40.0,
  maxPredictionAgeSeconds: Long = 86400L // 24 hours default max age
)

/**
 * Singleton entry point for the closed-loop forecast-to-reroute engine.
 * In Scala, an `object` is a singleton instance (static methods and state in Java).
 */
object RouteRecommender {

  /**
   * The documented free-flow fallback velocity applied when a zone lacks recent model forecasts.
   * Documented clearly as a default heuristic, never disguised as a model output.
   */
  val FREE_FLOW_FALLBACK_KMPH: Double = 40.0

  /**
   * Resolves a file path across typical working directories (repo root, submodules, tests).
   * For example, whether sbt runs from repo root or inside `services/graphx-reroute/`,
   * this helper searches multiple parent relative levels until it finds the existing file.
   *
   * @param candidates Sequence of relative file path attempts.
   * @return The first path that physically exists on disk, or the first candidate as fallback.
   */
  def resolvePath(candidates: Seq[String]): String = {
    // Expand each candidate into candidate, ../../candidate, and ../candidate
    val expanded = candidates.flatMap { p =>
      if (new File(p).isAbsolute) Seq(p)
      else Seq(p, s"../../$p", s"../$p")
    }
    // Find the first path that physically exists on the local filesystem
    expanded.find(p => new File(p).exists()).getOrElse(candidates.head)
  }

  /**
   * Resolves output path so that if running inside a submodule like services/graphx-reroute,
   * output lands in the canonical repository root data/ folder.
   *
   * @param target Desired output directory path.
   * @return Adjusted path ensuring Parquet files land in the project root's `data/` folder.
   */
  def resolveOutputPath(target: String): String = {
    if (new File(target).isAbsolute) {
      target
    } else if (new File("../../data").exists() && !new File("data/feature_table").exists()) {
      s"../../$target"
    } else if (new File("../data").exists() && !new File("data/feature_table").exists()) {
      s"../$target"
    } else {
      target
    }
  }

  /**
   * Builds or reuses a SparkSession configured for local GraphX execution.
   * Disables the web UI and forces localhost binding for clean, headless execution in tests and CI.
   *
   * @param appName Human-readable application name displayed in Spark logs.
   * @return Active SparkSession instance.
   */
  def getOrCreateSparkSession(appName: String = "UrbanixRouteRecommender"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      // "local[*]" instructs Spark to use all available CPU cores on the local machine
      .master("local[*]")
      // Disable the web UI server to eliminate port conflict warnings during test runs
      .config("spark.ui.enabled", "false")
      // Explicitly bind driver to loopback interface (prevents Windows DNS/firewall lookup delays)
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Standardize timestamp serialization to UTC timezone across all JVMs
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  /**
   * Reads and extracts the single latest valid prediction record for each zone
   * from the shared Parquet feature table.
   *
   * Rigorous Design Rules:
   * 1. Deterministic Selection: Uses a Window function partitioned by `zone_id` and ordered
   *    by timestamp descending. We do NOT use non-deterministic `max(window)` + `last(speed)`.
   * 2. Physical Bounds Validation: Speeds must satisfy 0.0 < speed <= 250.0 km/h.
   * 3. Null & NaN Rejection: Null or non-finite values are filtered prior to window ranking.
   * 4. Schema Flexibility: Supports both `predicted_speed_kmph` and `predicted_speed` column names,
   *    as well as either `window_start`, `window.start`, or `prediction_timestamp`.
   *
   * @param spark Active SparkSession.
   * @param featureTablePath Path to the Parquet directory written by Day 6.
   * @return PredictionExtractionSummary containing the extracted zone -> speed map.
   */
  def loadLatestZonePredictions(
    spark: SparkSession,
    featureTablePath: String
  ): PredictionExtractionSummary = {
    // Check if the requested directory or Parquet folder actually exists
    val tableFile = new File(featureTablePath)
    if (!tableFile.exists()) {
      println(s"[WARN] Feature table path does not exist: $featureTablePath. Using free-flow fallback for all zones.")
      return PredictionExtractionSummary(0L, 0, Map.empty, None, isFromFallback = true)
    }

    // Safely attempt to read the Parquet dataset using Spark SQL
    val rawDF = try {
      spark.read.parquet(featureTablePath)
    } catch {
      case ex: Exception =>
        println(s"[WARN] Could not read Parquet from $featureTablePath (${ex.getMessage}). Falling back to defaults.")
        return PredictionExtractionSummary(0L, 0, Map.empty, None, isFromFallback = true)
    }

    // Verify whether the DataFrame has any actual data partitions/rows
    if (rawDF.rdd.isEmpty()) {
      println(s"[WARN] Feature table at $featureTablePath is empty. Using free-flow fallback.")
      return PredictionExtractionSummary(0L, 0, Map.empty, None, isFromFallback = true)
    }

    // Count records and collect available column names into a Set for fast membership lookups
    val totalRead = rawDF.count()
    val columns = rawDF.columns.toSet

    // 1. Identify Zone ID column (Mandatory)
    if (!columns.contains("zone_id")) {
      println("[WARN] Feature table lacks required 'zone_id' column. Falling back.")
      return PredictionExtractionSummary(totalRead, 0, Map.empty, None, isFromFallback = true)
    }

    // 2. Identify Predicted Speed column (prefer predicted_speed_kmph, fallback to predicted_speed or avg_speed_kmph)
    val speedColName = if (columns.contains("predicted_speed_kmph")) {
      "predicted_speed_kmph"
    } else if (columns.contains("predicted_speed")) {
      "predicted_speed"
    } else if (columns.contains("avg_speed_kmph")) {
      "avg_speed_kmph"
    } else {
      println("[WARN] No recognized speed column found in feature table. Falling back.")
      return PredictionExtractionSummary(totalRead, 0, Map.empty, None, isFromFallback = true)
    }

    // 3. Identify Timestamp column for temporal ordering
    val hasWindowStart = columns.contains("window_start")
    val hasPredictionTimestamp = columns.contains("prediction_timestamp")
    val hasWindowStruct = columns.contains("window")

    // Filter out corrupted, missing, or physically impossible sensor/model readings
    var prepDF = rawDF.filter(
      col("zone_id").isNotNull &&
      length(trim(col("zone_id"))) > 0 &&
      col(speedColName).isNotNull &&
      !isnan(col(speedColName)) &&
      col(speedColName) > 0.0 &&
      col(speedColName) <= 250.0
    )

    // Standardize time column as `ordering_time` so the window function can sort reliably
    prepDF = if (hasWindowStart) {
      prepDF.withColumn("ordering_time", col("window_start").cast(TimestampType))
    } else if (hasPredictionTimestamp) {
      prepDF.withColumn("ordering_time", col("prediction_timestamp").cast(TimestampType))
    } else if (hasWindowStruct) {
      prepDF.withColumn("ordering_time", col("window.start").cast(TimestampType))
    } else {
      // Deterministic fallback if no timestamp column exists: epoch 0 (1970-01-01)
      prepDF.withColumn("ordering_time", lit(new Timestamp(0L)))
    }

    // Standardize tie-breaker column (if two predictions have identical timestamps, use sample count)
    val hasRecordCount = columns.contains("traffic_record_count")
    prepDF = if (hasRecordCount) {
      prepDF.withColumn("tie_breaker", col("traffic_record_count").cast(LongType))
    } else {
      prepDF.withColumn("tie_breaker", lit(0L))
    }

    // 4. Deterministic Latest Selection via Window Partitioning
    // Window partitioned by zone_id, ordered by ordering_time DESC, then tie_breaker DESC
    // This groups rows by zone, and ranks the freshest observation as row #1.
    val windowSpec = Window.partitionBy("zone_id").orderBy(col("ordering_time").desc, col("tie_breaker").desc)

    // Assign rank 1, 2, ... within each zone, keep only rank 1, and select essential columns
    val latestPerZoneDF = prepDF
      .withColumn("row_rank", row_number().over(windowSpec))
      .filter(col("row_rank") === 1)
      .select("zone_id", speedColName, "ordering_time")

    // Collect the ranked zone records to the Spark driver as an array of Rows
    val collectedRows = latestPerZoneDF.collect()

    // Transform Spark Rows into a clean, strongly typed Scala Map[String, Double]
    val zoneMap: Map[String, Double] = collectedRows.map { row =>
      val zone = row.getAs[String]("zone_id")
      val speed = row.getAs[Number](speedColName).doubleValue()
      zone -> speed
    }.toMap

    // Find the latest timestamp among all returned prediction records
    val latestTimestamp: Option[Timestamp] = if (collectedRows.nonEmpty) {
      val maxTs = collectedRows.map(_.getAs[Timestamp]("ordering_time")).filter(_ != null).reduceOption((a, b) => if (a.after(b)) a else b)
      maxTs
    } else None

    // Return the complete extraction summary
    PredictionExtractionSummary(
      totalRecordsRead = totalRead,
      distinctZonesCount = zoneMap.size,
      zoneSpeeds = zoneMap,
      latestPredictionTime = latestTimestamp,
      isFromFallback = zoneMap.isEmpty
    )
  }

  /**
   * Applies the zone-to-edge speed mapping and recomputes travel-time weights on all directed road edges.
   *
   * Mapping Rule:
   * Each directed edge (src -> dst) adopts the predicted velocity of the DESTINATION intersection's zone.
   * If the destination zone has no prediction, FREE_FLOW_FALLBACK_KMPH is used.
   *
   * Travel Time Formula:
   * travel_time_hours = base_distance_km / effective_speed_kmph
   * Clamped by CongestionWeight's gridlock floor (1.0 km/h) to prevent division by zero.
   *
   * @param rawGraph Base GraphX property graph with base distance attributes.
   * @param intersectionsDF DataFrame containing intersection metadata (id, zone_id).
   * @param roadsDF DataFrame containing road edges (src_id, dst_id, base_distance_km).
   * @param zoneSpeedMap Map of zone_id -> predicted speed.
   * @param fallbackSpeedKmph Default speed to apply when zone prediction is absent.
   * @return Tuple of (Reweighted GraphX Graph, EdgeMappingSummary metrics).
   */
  def reweightGraphWithPredictions(
    rawGraph: Graph[String, Double],
    intersectionsDF: DataFrame,
    roadsDF: DataFrame,
    zoneSpeedMap: Map[String, Double],
    fallbackSpeedKmph: Double = FREE_FLOW_FALLBACK_KMPH
  ): (Graph[String, Double], EdgeMappingSummary) = {

    // 1. Build Intersection -> Zone lookup map
    // We collect the metadata into a Scala Map on driver memory (20 intersections = tiny footprint)
    val intersectionZoneMap: Map[VertexId, String] = intersectionsDF
      .filter(col("id").isNotNull && col("zone_id").isNotNull)
      .collect()
      .map { row =>
        val id = row.getAs[Number]("id").longValue()
        val zoneId = row.getAs[String]("zone_id")
        id -> zoneId
      }
      .toMap

    // 2. Transform graph edge weights using destination zone speed
    // mapTriplets iterates over each edge along with its source and destination vertex attributes.
    val reweightedGraph: Graph[String, Double] = rawGraph.mapTriplets { triplet =>
      val dstId = triplet.dstId
      val baseDistanceKm = triplet.attr // Original edge attribute is distance in kilometers

      // Identify which zone the destination intersection belongs to
      val dstZone = intersectionZoneMap.getOrElse(dstId, "UNKNOWN")
      val (speedToApply, _) = zoneSpeedMap.get(dstZone) match {
        case Some(speed) => (speed, true)
        case None => (fallbackSpeedKmph, false)
      }

      // Defensive calculation with gridlock floor clamping (1.0 km/h) via CongestionWeight helper
      CongestionWeight.calculateTravelTimeHours(baseDistanceKm, speedToApply)
    }

    // Materialize metrics across the reweighted graph edges
    val edgeAttributes = reweightedGraph.edges.map(_.attr).collect()
    val totalEdges = edgeAttributes.length.toLong

    // Calculate how many edges used real MLlib predictions vs default free-flow fallback
    val edgeDestinations = rawGraph.edges.map(e => (e.dstId, e.attr)).collect()
    var predEdges = 0L
    var fallEdges = 0L
    var totalSpeedSum = 0.0

    edgeDestinations.foreach { case (dstId, _) =>
      val dstZone = intersectionZoneMap.getOrElse(dstId, "UNKNOWN")
      if (zoneSpeedMap.contains(dstZone)) {
        predEdges += 1
        totalSpeedSum += zoneSpeedMap(dstZone)
      } else {
        fallEdges += 1
        totalSpeedSum += fallbackSpeedKmph
      }
    }

    // Calculate statistical averages and min/max travel time values
    val avgSpeed = if (totalEdges > 0) totalSpeedSum / totalEdges.toDouble else 0.0
    val minTime = if (edgeAttributes.nonEmpty) edgeAttributes.min else 0.0
    val maxTime = if (edgeAttributes.nonEmpty) edgeAttributes.max else 0.0

    // Package the summary metrics
    val summary = EdgeMappingSummary(
      totalEdges = totalEdges,
      predictedEdgesCount = predEdges,
      fallbackEdgesCount = fallEdges,
      averageSpeedKmph = avgSpeed,
      minTravelTimeHours = minTime,
      maxTravelTimeHours = maxTime
    )

    (reweightedGraph, summary)
  }

  /**
   * Computes shortest routes from origin to all destinations, formats results, and
   * writes the snapshot to the target Parquet directory using OVERWRITE mode.
   *
   * @param spark Active SparkSession.
   * @param weightedGraph Graph with travel-time weights in hours.
   * @param originId Origin intersection ID (e.g. 1L).
   * @param outputPath Target directory for recommended routes.
   * @param activeZonesCount Number of active prediction zones applied.
   * @param fallbackUsed True if any fallback speed was utilized in the network.
   * @param computedAt Timestamp of calculation.
   * @return DataFrame representing the persisted route snapshot.
   */
  def computeAndPublishRoutes(
    spark: SparkSession,
    weightedGraph: Graph[String, Double],
    originId: Long,
    outputPath: String,
    activeZonesCount: Int,
    fallbackUsed: Boolean,
    computedAt: Timestamp = new Timestamp(System.currentTimeMillis())
  ): DataFrame = {
    // Import implicits to enable .toDF() conversion on Scala case class collections
    import spark.implicits._

    // Execute Day 8 Pregel SSSP algorithm (Bellman-Ford message passing)
    val routesMap = ShortestPathPregel.computeRoutes(weightedGraph, originId)

    // Format output records into RecommendedRouteRow instances
    val routeRows = routesMap.toSeq.sortBy(_._1).map { case (destId, routeRes) =>
      val pathArray = routeRes.path.toArray
      val pathFormatted = if (routeRes.isReachable) routeRes.path.mkString(" -> ") else "UNREACHABLE"
      val timeMinutes = routeRes.totalTravelTimeMinutes

      RecommendedRouteRow(
        origin_id = originId,
        destination_id = destId,
        path = pathArray,
        path_formatted = pathFormatted,
        total_travel_time_hours = routeRes.totalTravelTimeHours,
        total_travel_time_minutes = if (routeRes.isReachable) timeMinutes else Double.PositiveInfinity,
        is_reachable = routeRes.isReachable,
        computed_at = computedAt,
        prediction_zones_used = activeZonesCount,
        fallback_applied = fallbackUsed
      )
    }

    // Convert the sequence of case classes into a Spark SQL DataFrame
    val routeDF = routeRows.toDF()

    // Write to Parquet using OVERWRITE mode:
    // Only the CURRENT optimal recommendation snapshot is relevant to the dashboard.
    // Commuters require the freshest decision right now, not a historical log of old paths.
    routeDF.write
      .mode("overwrite")
      .parquet(outputPath)

    routeDF
  }

  /**
   * Executes a single complete end-to-end iteration of the recommendation workflow.
   * 1. Resolves CSV and Parquet file paths.
   * 2. Loads static road network and constructs base GraphX property graph.
   * 3. Scans shared Parquet feature table for the latest MLlib velocity forecasts.
   * 4. Reweights road edges by applying destination-zone speeds.
   * 5. Solves Pregel SSSP and publishes recommended route snapshot to Parquet.
   *
   * @param spark SparkSession.
   * @param config Application configuration.
   * @return The published route DataFrame.
   */
  def executeIteration(
    spark: SparkSession,
    config: RecommenderConfig
  ): DataFrame = {
    // Capture start time for performance tracking
    val iterationStart = Instant.now()

    // 1. Resolve source data paths across repository subdirectories
    val intersectionsPath = resolvePath(Seq(
      config.intersectionsPath,
      "data/intersections.csv",
      "../../data/intersections.csv",
      "../data/intersections.csv"
    ))
    val roadsPath = resolvePath(Seq(
      config.roadsPath,
      "data/roads.csv",
      "../../data/roads.csv",
      "../data/roads.csv"
    ))
    val featureTablePath = resolvePath(Seq(
      config.featureTablePath,
      "data/feature_table/traffic",
      "../../data/feature_table/traffic",
      "../data/feature_table/traffic"
    ))

    // 2. Load static road network CSVs and validate graph topology
    val (intersectionsDF, roadsDF) = RoadGraphBuilder.loadDataFrames(spark, intersectionsPath, roadsPath)
    val (rawGraph, _) = RoadGraphBuilder.validateAndBuildGraph(spark, intersectionsDF, roadsDF, strict = true)

    // 3. Extract latest predictions from shared Parquet store
    val predSummary = loadLatestZonePredictions(spark, featureTablePath)

    // 4. Reweight graph using destination zone mapping
    val (weightedGraph, mapSummary) = reweightGraphWithPredictions(
      rawGraph = rawGraph,
      intersectionsDF = intersectionsDF,
      roadsDF = roadsDF,
      zoneSpeedMap = predSummary.zoneSpeeds,
      fallbackSpeedKmph = config.freeFlowFallbackKmph
    )

    // 5. Compute and publish routes
    val computedAt = new Timestamp(System.currentTimeMillis())
    val resolvedTarget = resolveOutputPath(config.outputPath)
    val routeDF = computeAndPublishRoutes(
      spark = spark,
      weightedGraph = weightedGraph,
      originId = config.originId,
      outputPath = resolvedTarget,
      activeZonesCount = predSummary.distinctZonesCount,
      fallbackUsed = mapSummary.fallbackEdgesCount > 0,
      computedAt = computedAt
    )

    // Log diagnostic summary for operations team
    val reachable = routeDF.filter(col("is_reachable") === true).count()
    println(
      s"[$computedAt] Recomputed ${routeDF.count()} routes ($reachable reachable) from Origin ${config.originId} | " +
      s"Zones: ${predSummary.distinctZonesCount} active | " +
      s"Edges: ${mapSummary.predictedEdgesCount} predicted, ${mapSummary.fallbackEdgesCount} fallback | " +
      s"Avg Speed: ${f"${mapSummary.averageSpeedKmph}%.1f"} km/h"
    )

    routeDF
  }

  /**
   * Runs the continuous polling loop.
   * Periodically wakes up, checks the Parquet feature store, recalculates routes,
   * and sleeps for `pollIntervalSeconds`.
   *
   * @param spark SparkSession.
   * @param config Application configuration.
   */
  def runPollingLoop(spark: SparkSession, config: RecommenderConfig): Unit = {
    println("=" * 80)
    println("  URBANIX DAY 9: ROUTE RECOMMENDER (POLLING MODE)")
    println(s"  Origin: ${config.originId} | Poll Interval: ${config.pollIntervalSeconds}s | Target: ${config.outputPath}")
    println("=" * 80)

    var running = true
    // Register clean JVM shutdown hook so Ctrl+C gracefully exits without leaving orphaned locks
    sys.addShutdownHook {
      println("\n[INFO] RouteRecommender received shutdown signal. Terminating gracefully...")
      running = false
    }

    var cycle = 1
    // Polling loop: continuously updates routing recommendations
    while (running) {
      try {
        println(s"\n--- [Cycle $cycle] Evaluating latest forecast snapshot ---")
        executeIteration(spark, config)
        cycle += 1
      } catch {
        case ex: InterruptedException =>
          println("[INFO] Polling loop interrupted. Exiting.")
          running = false
        case ex: Throwable =>
          // Catch unexpected errors and log without crashing the continuous service
          println(s"[ERROR] Transient failure during routing iteration: ${ex.getMessage}")
          ex.printStackTrace()
          println("[INFO] Retrying on next scheduled poll cycle...")
      }

      // Sleep until the next polling cycle unless interrupted
      if (running) {
        try {
          Thread.sleep(config.pollIntervalSeconds * 1000L)
        } catch {
          case _: InterruptedException =>
            println("[INFO] Sleep interrupted. Shutting down polling loop.")
            running = false
        }
      }
    }
  }

  /**
   * Command-line argument parser.
   * Inspects args array for flags such as `--once`, `--poll <sec>`, `--origin <id>`, etc.
   *
   * @param args Command-line arguments passed to the JVM.
   * @return Populated RecommenderConfig instance.
   */
  def parseArgs(args: Array[String]): RecommenderConfig = {
    var config = RecommenderConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--once" =>
          config = config.copy(runOnce = true)
        case "--poll" if i + 1 < args.length =>
          config = config.copy(pollIntervalSeconds = args(i + 1).toInt)
          i += 1
        case "--origin" if i + 1 < args.length =>
          config = config.copy(originId = args(i + 1).toLong)
          i += 1
        case "--feature-table" if i + 1 < args.length =>
          config = config.copy(featureTablePath = args(i + 1))
          i += 1
        case "--output" if i + 1 < args.length =>
          config = config.copy(outputPath = args(i + 1))
          i += 1
        case "--fallback-speed" if i + 1 < args.length =>
          config = config.copy(freeFlowFallbackKmph = args(i + 1).toDouble)
          i += 1
        case other =>
          println(s"[WARN] Unrecognized CLI option: $other")
      }
      i += 1
    }
    config
  }

  /**
   * Application entry point.
   * Initializes Spark, parses command-line parameters, and triggers either single-pass or polling mode.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    val spark = getOrCreateSparkSession()

    try {
      if (config.runOnce) {
        // Single-pass mode: used by batch cron jobs or automated tests
        println("=" * 80)
        println("  URBANIX DAY 9: ROUTE RECOMMENDER (SINGLE-PASS EXECUTION)")
        println("=" * 80)
        val df = executeIteration(spark, config)
        println(s"\n[SUCCESS] Route snapshot successfully published to ${config.outputPath}")
        println(s"Total routes: ${df.count()}")
      } else {
        // Continuous polling mode: standard production operation
        runPollingLoop(spark, config)
      }
    } finally {
      // Ensure SparkSession stops and releases CPU/RAM resources cleanly
      println("\nCleaning up SparkSession resources...")
      spark.stop()
      println("[OK] SparkSession stopped cleanly.")
    }
  }
}
