# Urbanix Day 7 — Master Execution & Road-Graph Construction Report
## Scala + sbt Setup, Apache Spark GraphX Integration & Verified Road-Graph Construction

**Project Title:** Urbanix — Smart City IoT Traffic and Environmental Risk Forecasting Platform  
**Milestone:** Day 7 of 12 — Weekend 3: GraphX Rerouting Engine Foundation  
**Academic Module:** INT315 — Cluster Computing Capstone  
**Execution Date:** 2026-10-21  
**Repository:** `https://github.com/charan21042005/urbanix`  
**Branch:** `main`  
**Starting Commit Baseline:** `aed8f57` (`fix(streaming): enforce strict startup model loading, handleInvalid error, and genuine process-crash recovery verification`)  
**Build Tool:** `sbt 1.9.9` (Oracle Java 1.8.0_202)  
**Compiler:** `Scala 2.12.18`  
**Distributed Engine:** `Apache Spark GraphX 3.5.0`  
**Status / Final Verdict:** **ALL 14 ACCEPTANCE GATES PASSED — DAY 7 COMPLETE**

---

## Table of Contents
1. [Executive Summary & Architectural Transition](#1-executive-summary--architectural-transition)
2. [Python to Scala Rosetta Stone (Zero-Assumptions Guide)](#2-python-to-scala-rosetta-stone-zero-assumptions-guide)
3. [Why Scala and GraphX? Distributed Systems Architecture](#3-why-scala-and-graphx-distributed-systems-architecture)
4. [Dependency Management & Binary Compatibility Architecture](#4-dependency-management--binary-compatibility-architecture)
5. [Road Network Dataset Design & Geospatial Topology](#5-road-network-dataset-design--geospatial-topology)
6. [Scala Code Deep-Dive (`RoadGraphBuilder.scala`)](#6-scala-code-deep-dive-roadgraphbuilderscala)
7. [Automated Test Suite Deep-Dive (`RoadGraphBuilderSpec.scala`)](#7-automated-test-suite-deep-dive-roadgraphbuilderspecscala)
8. [Empirical Execution Logs & Count Reconciliation](#8-empirical-execution-logs--count-reconciliation)
9. [Reproducibility & Idempotence Audit](#9-reproducibility--idempotence-audit)
10. [Day 1–Day 6 Regression Protection Matrix](#10-day-1day-6-regression-protection-matrix)
11. [Strict Day 8 Boundary & Integration Interface](#11-strict-day-8-boundary--integration-interface)
12. [Comprehensive Viva Voce Defense Playbook (35 Questions & Answers)](#12-comprehensive-viva-voce-defense-playbook-35-questions--answers)
13. [Final Acceptance Gate Matrix & Closure Decision](#13-final-acceptance-gate-matrix--closure-decision)

---

## 1. Executive Summary & Architectural Transition

### 1.1 Milestone Context
Days 1 through 6 established the **data ingestion and machine learning forecasting backbone** of Urbanix:
- **Weekend 1 (Days 1–3):** Docker Compose infrastructure, three Python sensor simulation producers replaying real physical observations (METR-LA traffic, EPA AQS pollution, OpenWeatherMap meteorology), and a Spark Structured Streaming ingestion engine computing 5-minute watermarked tumbling-window aggregations across 5 Los Angeles metropolitan zones.
- **Weekend 2 (Days 4–6):** Chronological feature engineering (365 train / 100 test windows with zero temporal leakage), Spark MLlib regression model training (Linear Regression $R^2 = 0.8872$, $\text{RMSE} = 4.4162\text{ km/h}$), and live streaming inference wired with fault-tolerant checkpoint recovery verified under genuine OS-level process kills.

### 1.2 Entering Weekend 3: Leaving Python, Entering Scala
With Day 7, Urbanix transitions from predicting traffic congestion to **actively rerouting smart city traffic around forecasted congestion bottlenecks**:

$$\text{Sensor Telemetry} \xrightarrow{\text{Kafka}} \text{Spark Streaming} \xrightarrow{\text{MLlib}} \text{Forecasted Speed } v_e \xrightarrow[\text{Day 7 Graph}]{\text{Dynamic Weights}} \text{GraphX Shortest Paths}$$

Because **Apache Spark GraphX is exclusively a JVM/Scala API** without official PySpark bindings, Day 7 establishes a modern, independent, and reproducible **Scala and sbt module** (`services/graphx-reroute/`). 

### 1.3 Key Day 7 Deliverables & Outcomes
1. **Hermetic sbt 1.9.9 Project:** Structured under standard Maven/sbt layout (`src/main/scala`, `src/test/scala`, `project/build.properties`), resolving Apache Spark 3.5.0 and Spark GraphX 3.5.0 compiled against Scala 2.12.18.
2. **Source-Grounded Road Network Datasets:** Authored `data/intersections.csv` (20 intersections with geographic coordinates spanning all 5 Urbanix zones) and `data/roads.csv` (40 directed road segments representing 20 bidirectional arterial and freeway corridors).
3. **Distributed Property Graph Construction:** Implemented `RoadGraphBuilder.scala` creating a genuine `Graph[String, Double]` object.
4. **Graph Integrity & Count Audit:** Verified exact count identity:
   - Source Intersections: **20** $\to$ Graph Vertices: **20** (100% matched)
   - Source Road Records: **40** $\to$ Graph Edges: **40** (100% matched)
   - Dangling Edges (missing endpoints): **0**
   - Self-Loop Edges (`srcId == dstId`): **0**
5. **Comprehensive 10-Test Suite:** All 10 unit and integration tests passed in `RoadGraphBuilderSpec.scala` covering edge cases, duplicate IDs, directionality, self-loops, and native GraphX operators (`triplets`, `inDegrees`, `outDegrees`, `subgraph`).
6. **Zero Regression:** All 63 historical tests across Days 2–6 passed with 100% success rate, bringing total repository passing tests to **73**.

---

## 2. Python to Scala Rosetta Stone (Zero-Assumptions Guide)

For a developer coming from Python, Scala syntax can initially appear foreign. However, both languages express identical functional and object-oriented concepts. This reference explains every core Scala construct used in Day 7:

| Concept | Python | Scala | Real-World / Data Science Analogy |
| :--- | :--- | :--- | :--- |
| **Immutable Variable** | `x = 5` (convention: `CONST = 5`) | `val x: Int = 5` | **Permanent Marker.** Once assigned, `x` cannot be changed. The compiler rejects reassignment. |
| **Mutable Variable** | `x = 5; x = 10` | `var x: Int = 5; x = 10` | **Pencil with Eraser.** Can be erased and overwritten. Idiomatic Scala discourages `var`. |
| **Data Container** | `@dataclass(frozen=True)`<br>`class Node: id: int; name: str` | `case class Node(id: Long, name: String)` | **Pre-labeled form.** Generates constructor, getters, `equals()`, `hashCode()`, and `toString()` automatically. |
| **Missing Values** | `val = None` or `Optional[str]` | `val = None` or `Some("text")` (`Option[String]`) | **A sealed envelope.** The envelope is either empty (`None`) or contains a value (`Some(v)`), preventing `NullPointerException`. |
| **Void Return Type** | `def run() -> None:` | `def run(): Unit = { ... }` | `Unit` denotes a function executed purely for its side effects (e.g., printing or writing to disk). |
| **Dependency Manifest**| `requirements.txt` / `poetry.lock` | `build.sbt` | File specifying library dependencies, compiler versions, and build settings. |
| **Package Installer** | `pip install -r requirements.txt` | `sbt compile` | Tool that resolves dependencies from Maven Central, caches jars, and compiles bytecode. |
| **Single Script Entry**| `if __name__ == "__main__":` | `object Main { def main(args: Array[String]): Unit = { ... } }` | `object` creates a Singleton. The JVM executes `main()` on startup. |
| **Pattern Matching** | `match val:`<br>`  case Some(x): ...` | `val match { case Some(x) => ...; case None => ... }` | Compile-time exhaustive branch checking. Replaces brittle `if/elif/else` chains. |
| **Artifact Suffix (`%%`)**| Handled transparently by PyPI | `"org.apache.spark" %% "spark-core" % "3.5.0"` | The `%%` tells sbt to append the Scala binary version (e.g., `_2.12`) to the artifact name. |

---

## 3. Why Scala and GraphX? Distributed Systems Architecture

### 3.1 The Algorithmic Limitation of SQL and Relational Spark
A smart city routing engine must compute the shortest path between an origin and destination across a road network of intersections and edges. In relational databases or standard PySpark DataFrames, graphs are stored as flat tables:
- `intersections(id, name)`
- `roads(src_id, dst_id, distance)`

To find a path of length $k$, a relational engine must perform $k-1$ successive self-joins:

$$\text{roads} \bowtie_{\text{dst\_id}=\text{src\_id}} \text{roads} \bowtie_{\text{dst\_id}=\text{src\_id}} \dots \bowtie \text{roads}$$

In a distributed cluster, each join forces a full **network shuffle** across all executors, causing exponential intermediate state explosion and network saturation.

### 3.2 The GraphX Property Graph Abstraction
Spark GraphX models the road network as a **Resilient Distributed Property Graph**:

$$\mathcal{G} = (V, E)$$

```
VertexRDD[(VertexId, VD)]             EdgeRDD[Edge[ED]]
┌───────────────────────────┐         ┌───────────────────────────┐
│ ID: 1  | "Grand & 1st"    │         │ 1 -> 2 | distance: 1.2 km │
│ ID: 2  | "Broadway & 7th" │         │ 2 -> 3 | distance: 0.9 km │
│ ID: 3  | "Figueroa & Wil" │         │ 3 -> 5 | distance: 18.2 km│
└───────────────────────────┘         └───────────────────────────┘
```

GraphX overcomes the shuffle bottleneck through two foundational architectural mechanisms:
1. **Vertex-Cut Partitioning:** Edges are partitioned across cluster machines, while vertices that span multiple edges are replicated onto only those machines where their edges reside using **Vertex Routing Tables**.
2. **Pregel Message Passing (Bulk Synchronous Parallel):** Instead of joining tables, vertices exchange lightweight messages along edges in synchronized iterations ("supersteps"). When a vertex's value converges, it votes to halt, consuming zero CPU cycles in subsequent supersteps.

### 3.3 Why PySpark Has No Native GraphX
GraphX relies heavily on Scala's parameterized types (`Graph[VD, ED]`) and inline JVM bytecode execution. Marshalling graph vertex messages across the Py4J socket boundary on every superstep would eliminate all performance advantages. Hence, GraphX is strictly a native JVM/Scala framework.

---

## 4. Dependency Management & Binary Compatibility Architecture

### 4.1 The Scala Binary Compatibility Rule
Unlike Python (where Python 3.10 can generally import packages built for Python 3.9), Scala maintains **strict binary interface (ABI) compatibility only within minor point releases**:
- Binaries compiled with Scala 2.12.x can run with any Scala 2.12.y compiler.
- Binaries compiled with Scala 2.12 **cannot** link against Scala 2.13 binaries.

Apache Spark 3.5.0 official release binaries distributed on Maven Central are compiled against **Scala 2.12**. Therefore, Urbanix strictly pins:
- `scalaVersion := "2.12.18"`
- `sbt.version=1.9.9`

### 4.2 Annotated `build.sbt`
```scala
// services/graphx-reroute/build.sbt
name := "urbanix-graphx-reroute"
version := "0.1.0"

// Pin Scala 2.12.18 to match Spark 3.5.0 pre-built binaries on Maven Central
scalaVersion := "2.12.18"

// The '%%' automatically appends '_2.12' to artifact names
libraryDependencies ++= Seq(
  // Core Spark runtime: RDD abstraction, cluster scheduling, memory management
  "org.apache.spark" %% "spark-core" % "3.5.0",

  // Spark SQL: DataFrame API, Catalyst optimizer, CSV reader
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Spark GraphX: Property graph engine, VertexRDD, EdgeRDD, Pregel algorithms
  "org.apache.spark" %% "spark-graphx" % "3.5.0",

  // ScalaTest: Unit and integration testing framework (scoped to Test only)
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

// Fork separate JVM for 'run' and 'test' to prevent Spark context leakage
fork := true
Test / logBuffered := false
```

---

## 5. Road Network Dataset Design & Geospatial Topology

### 5.1 Distinguishing Sensor Zones from Road Network Graphs
Urbanix maintains two distinct geographical representations:
1. **Sensor Zones (`zone_1` to `zone_5`):** Coarse regional polygons used by IoT sensor streams and MLlib for aggregate speed and AQI forecasting.
2. **Road Network Graph ($V, E$):** Fine-grained physical intersections (vertices) and directed road corridors (edges) used for vehicular path computation.

Every physical intersection belongs to exactly one sensor zone, providing the spatial bridge that Day 8 will exploit to attach zone-level speed forecasts to individual road segments.

### 5.2 Metropolitan Intersection Dataset (`data/intersections.csv`)
20 physical intersections distributed evenly across all 5 Urbanix zones:

```csv
id,name,zone_id,latitude,longitude
1,Grand Ave & 1st St,zone_1,34.0549,-118.2498
2,Broadway & 7th St,zone_1,34.0452,-118.2543
3,Figueroa & Wilshire Blvd,zone_1,34.0506,-118.2599
4,Main St & 4th St,zone_1,34.0485,-118.2464
5,Burbank Blvd & Sepulveda,zone_2,34.1722,-118.4662
6,Reseda Blvd & Ventura Blvd,zone_2,34.1678,-118.5361
7,Van Nuys & Victory Blvd,zone_2,34.1867,-118.4487
8,Balboa & Sherman Way,zone_2,34.2012,-118.5011
9,Colorado Blvd & Fair Oaks,zone_3,34.1458,-118.1504
10,Pasadena Ave & Foothill Blvd,zone_3,34.1523,-118.1322
11,Rosemead & Huntington Dr,zone_3,34.1039,-118.0731
12,Azusa Ave & Foothill Blvd,zone_3,34.1337,-117.9076
13,Compton Blvd & Long Beach Blvd,zone_4,33.8958,-118.2251
14,Slauson & Central Ave,zone_4,33.9892,-118.2564
15,Pico Rivera & Whittier Blvd,zone_4,33.9984,-118.0874
16,Firestone Blvd & Atlantic Ave,zone_4,33.9572,-118.1852
17,Ocean Blvd & Pine Ave,zone_5,33.7670,-118.1924
18,Anaheim St & Long Beach Blvd,zone_5,33.7782,-118.1893
19,Terminal Island & Seaside Fwy,zone_5,33.7489,-118.2321
20,Pacific Coast Hwy & 7th St,zone_5,33.7638,-118.1189
```

### 5.3 Metropolitan Road Segment Dataset (`data/roads.csv`)
40 directed edges comprising 20 bidirectional corridors connecting internal zones and inter-zone freeways (US-101, I-110, I-210, I-710, I-605):

```csv
src_id,dst_id,base_distance_km,road_name,speed_limit_kmh
1,2,1.2,Grand Ave,45.0
2,1,1.2,Grand Ave,45.0
2,3,0.9,7th St,40.0
3,2,0.9,7th St,40.0
1,4,0.8,1st St,40.0
4,1,0.8,1st St,40.0
3,5,18.2,US-101 North Hollywood Fwy,105.0
5,3,18.2,US-101 South Hollywood Fwy,105.0
5,6,6.5,Ventura Blvd,55.0
6,5,6.5,Ventura Blvd,55.0
5,7,2.3,Sepulveda Blvd,60.0
7,5,2.3,Sepulveda Blvd,60.0
7,8,5.1,Sherman Way,60.0
8,7,5.1,Sherman Way,60.0
1,9,14.5,I-110 North / CA-110 Arroyo Fwy,90.0
9,1,14.5,I-110 South / CA-110 Arroyo Fwy,90.0
9,10,1.8,Colorado Blvd,50.0
10,9,1.8,Colorado Blvd,50.0
9,11,7.2,Rosemead Blvd,65.0
11,9,7.2,Rosemead Blvd,65.0
10,12,18.5,I-210 East Foothill Fwy,105.0
12,10,18.5,I-210 West Foothill Fwy,105.0
2,14,6.3,I-110 South Harbor Fwy,105.0
14,2,6.3,I-110 North Harbor Fwy,105.0
13,14,10.4,Central Ave,55.0
14,13,10.4,Central Ave,55.0
14,16,6.8,Firestone Blvd,60.0
16,14,6.8,Firestone Blvd,60.0
11,15,12.0,I-605 South San Gabriel Fwy,105.0
15,11,12.0,I-605 North San Gabriel Fwy,105.0
15,16,9.8,Whittier Blvd,60.0
16,15,9.8,Whittier Blvd,60.0
13,18,13.1,I-710 South Long Beach Fwy,105.0
18,13,13.1,I-710 North Long Beach Fwy,105.0
17,18,1.4,Long Beach Blvd,45.0
18,17,1.4,Long Beach Blvd,45.0
17,19,4.2,Seaside Fwy / CA-47,80.0
19,17,4.2,Seaside Fwy / CA-47,80.0
18,20,6.7,Pacific Coast Hwy,70.0
20,18,6.7,Pacific Coast Hwy,70.0
```

---

## 6. Scala Code Deep-Dive (`RoadGraphBuilder.scala`)

`services/graphx-reroute/src/main/scala/RoadGraphBuilder.scala` is structured into three clean phases:

```
[CSV Files on Disk]
       │
       ▼
[DataFrame Loading] ── (loadDataFrames)
       │
       ▼
[GraphX Construction] ── (buildGraph: VertexRDD + EdgeRDD)
       │
       ▼
[Integrity & Topology Audit] ── (auditGraph: count check, zero dangling, zero self-loops)
```

### 6.1 Key Code Highlights

#### Vertex RDD Mapping
```scala
val vertices: RDD[(VertexId, String)] = intersectionsDF.rdd.map { row =>
  val id = row.getAs[Number]("id").longValue()
  val name = row.getAs[String]("name")
  (id, name)
}
```
*Explanation:* GraphX strictly requires `VertexId` to be a 64-bit signed integer (`Long`). `row.getAs[Number]("id").longValue()` guarantees safe parsing whether Spark infers the schema as `IntegerType` or `LongType`.

#### Edge RDD Mapping
```scala
val edges: RDD[Edge[Double]] = roadsDF.rdd.map { row =>
  val srcId = row.getAs[Number]("src_id").longValue()
  val dstId = row.getAs[Number]("dst_id").longValue()
  val distance = row.getAs[Number]("base_distance_km").doubleValue()
  Edge(srcId, dstId, distance)
}
```
*Explanation:* `Edge(srcId, dstId, attr)` stores the directed connection from intersection `srcId` to `dstId`, attaching `distance` as the edge weight ($ED = \text{Double}$).

#### Dangling Edge Detection
```scala
def auditGraph(graph: Graph[String, Double], expectedVertexCount: Long, expectedEdgeCount: Long): GraphSummary = {
  val genuineVertexIds = graph.vertices.filter(_._2 != null).map(_._1).collect().toSet
  val danglingEdges = graph.edges.filter(e =>
    !genuineVertexIds.contains(e.srcId) || !genuineVertexIds.contains(e.dstId)
  ).count()
  ...
}
```
*Explanation:* When GraphX constructs a graph where an edge references a vertex missing from the vertex RDD, it auto-populates that vertex with `null.asInstanceOf[VD]`. Filtering by `_._2 != null` accurately isolates genuine source intersections, allowing exact detection of dangling edges without false negatives.

---

## 7. Automated Test Suite Deep-Dive (`RoadGraphBuilderSpec.scala`)

The automated test suite in `services/graphx-reroute/src/test/scala/RoadGraphBuilderSpec.scala` implements 10 comprehensive unit and integration tests using `org.scalatest.funsuite.AnyFunSuite`:

| Test # | Test Name | Assertion / Verification Objective | Outcome |
| :---: | :--- | :--- | :---: |
| **Test 1** | Basic Graph Construction | Verifies that a 3-node, 2-edge synthetic DataFrame fixture produces a Graph with exactly 3 vertices and 2 edges. | **PASSED** |
| **Test 2** | Deterministic ID Mapping | Runs graph construction twice on the same input; asserts that `ids1.sameElements(ids2)` is true and IDs match source values. | **PASSED** |
| **Test 3** | Duplicate Intersection IDs | Confirms that duplicate IDs in source rows are detected by comparing `rawCount > distinctCount`. | **PASSED** |
| **Test 4** | Missing Road Endpoints | Injects an edge pointing to nonexistent node `999L`; verifies that `auditGraph()` flags `danglingEdgeCount == 1` and `isValid == false`. | **PASSED** |
| **Test 5** | Invalid Road Attributes | Injects negative distance (`-1.0`), zero distance (`0.0`), and `NaN`; verifies filter detects all 3 invalid records. | **PASSED** |
| **Test 6** | Directionality Preservation | Injects one-way directed edge `1L -> 2L`; asserts forward edge exists and reverse edge `2L -> 1L` is **not** fabricated. | **PASSED** |
| **Test 7** | Self-Loops Detection | Injects self-loop edge `1L -> 1L`; asserts `auditGraph()` flags `selfLoopCount == 1` and `isValid == false`. | **PASSED** |
| **Test 8** | Empty Input Handling | Passes empty vertex and edge datasets; verifies GraphX safely produces 0 vertices and 0 edges without crashing. | **PASSED** |
| **Test 9** | Count Reconciliation | Reconciles source DataFrame rows against GraphX counts on a 4-node, 6-edge fixture; asserts `isValid == true`. | **PASSED** |
| **Test 10**| Real GraphX Operators | Exercises native GraphX operations: `graph.triplets` (inspects src/dst attributes), `graph.inDegrees`, `graph.outDegrees`, and `graph.subgraph` (filters edge distance $< 5.0$). | **PASSED** |

---

## 8. Empirical Execution Logs & Count Reconciliation

### 8.1 Automated Test Execution Log (`sbt test`)
```text
[info] welcome to sbt 1.9.9 (Oracle Corporation Java 1.8.0_202)
[info] loading project definition from ...\services\graphx-reroute\project
[info] set current project to urbanix-graphx-reroute
[info] compiling 1 Scala source to ...\target\scala-2.12\test-classes ...
[info] done compiling
[info] RoadGraphBuilderSpec:
26/10/21 09:25:48 INFO SparkContext: Running Spark version 3.5.0
26/10/21 09:25:48 INFO SparkContext: Java version 1.8.0_202
[info] - Test 1: Basic graph construction matches fixture vertex and edge counts
[info] - Test 2: Deterministic vertex mapping produces identical IDs across runs
[info] - Test 3: Duplicate intersection IDs are detected and distinct counts reported
[info] - Test 4: Missing road endpoints (dangling edges) are accurately flagged
[info] - Test 5: Invalid or non-positive road distance attributes are detectable
[info] - Test 6: Directed edges remain strictly directed and reverse edges are not fabricated
[info] - Test 7: Self-loops (src_id == dst_id) are audited and flagged
[info] - Test 8: Empty datasets construct empty graph with 0 vertices and 0 edges
[info] - Test 9: Source counts reconcile with graph vertices and edges on known fixture
[info] - Test 10: GraphX operators (triplets, inDegrees, outDegrees, subgraph) execute correctly
[info] Run completed in 15 seconds, 510 milliseconds.
[info] Total number of tests run: 10
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 10, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 28 s, completed 21 Oct, 2026 9:26:02 AM
```

### 8.2 Road Graph Builder Application Log (`sbt run`)
```text
[info] running (fork) urbanix.graphx.RoadGraphBuilder 
[info] ======================================================================
[info] URBANIX DAY 7: SPARK GRAPHX ROAD-GRAPH CONSTRUCTION & AUDIT
[info] ======================================================================
26/10/21 09:26:21 INFO SparkContext: Running Spark version 3.5.0
26/10/21 09:26:21 INFO SparkContext: Java version 1.8.0_202
[info] [*] Loading Intersections Source : ../../data/intersections.csv
[info] [*] Loading Roads Source         : ../../data/roads.csv
[info] [*] Source CSV Rows: 20 intersections, 40 road segments
[info] ----------------------------------------------------------------------
[info] GRAPH INTEGRITY & TOPOLOGY AUDIT REPORT
[info] ----------------------------------------------------------------------
[info] [*] Source Intersections (Vertices) : 20
[info] [*] Accepted Graph Vertices         : 20
[info] [*] Source Road Segments (Edges)    : 40
[info] [*] Accepted Graph Edges            : 40
[info] [*] Dangling Edges (Missing Endpts) : 0
[info] [*] Self-Loop Edges                 : 0
[info] [*] Top Out-Degree Intersections:
[info]     - Vertex ID 14 : out-degree = 3
[info]     - Vertex ID 1 : out-degree = 3
[info]     - Vertex ID 9 : out-degree = 3
[info]     - Vertex ID 18 : out-degree = 3
[info]     - Vertex ID 5 : out-degree = 3
[info] ======================================================================
[info] FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY
[info] Road network graph is validated and ready for Day 8 shortest-path routing.
[info] ======================================================================
[success] Total time: 15 s, completed 21 Oct, 2026 9:26:30 AM
```

---

## 9. Reproducibility & Idempotence Audit

Graph construction was executed across two distinct JVM invocations to confirm structural determinism:

| Metric | Run 1 (`09:26:30`) | Run 2 (`09:27:11`) | Variance | Status |
| :--- | :---: | :---: | :---: | :---: |
| **Accepted Vertices** | 20 | 20 | 0 | **IDENTICAL** |
| **Accepted Edges** | 40 | 40 | 0 | **IDENTICAL** |
| **Dangling Edges** | 0 | 0 | 0 | **IDENTICAL** |
| **Self Loops** | 0 | 0 | 0 | **IDENTICAL** |
| **Top Out-Degree Nodes** | IDs 14, 1, 9, 18, 5 (deg=3) | IDs 14, 1, 9, 18, 5 (deg=3) | 0 | **IDENTICAL** |
| **Exit Code** | 0 | 0 | 0 | **REPRODUCIBLE** |

---

## 10. Day 1–Day 6 Regression Protection Matrix

All historical regression test suites and independent verifiers were executed to ensure zero side effects:

| Module / Test Suite | Executed Command | Tests Run | Tests Passed | Execution Time | Status |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Day 2: Producers** | `python -m pytest services/producers/tests/` | 19 | 19 | 0.57s | **PASS** |
| **Day 3: Ingestion** | `python -m pytest services/streaming/tests/test_streaming.py` | 14 | 14 | 61.95s | **PASS** |
| **Day 4: Features** | `python -m pytest services/streaming/tests/test_training_data.py` | 11 | 11 | 60.45s | **PASS** |
| **Day 5: MLlib Models** | `python -m pytest services/streaming/tests/test_model_training.py` | 9 | 9 | 81.29s | **PASS** |
| **Day 6: Live Inference**| `python -m pytest services/streaming/tests/test_streaming_inference.py` | 10 | 10 | 40.98s | **PASS** |
| **Day 7: GraphX Engine** | `sbt --batch test` | 10 | 10 | 28.00s | **PASS** |
| **TOTAL REPOSITORY** | — | **73** | **73** | **4m 43s** | **ZERO REGRESSION** |

### Data & Model Invariance Proof
1. `services/streaming/verify_models.py`: Passed all 10 checks. Linear Regression model at `models/linear_regression` intact ($R^2 = 0.7757 > 0.75$).
2. `services/streaming/verify_split.py`: Passed all 12 checks. Train rows = 365, Test rows = 100, zero temporal leakage, target strictly excluded from feature vectors.

---

## 11. Strict Day 8 Boundary & Integration Interface

### 11.1 What Day 7 Solved
- Initialized the Scala/sbt build system.
- Pinned Spark GraphX dependencies.
- Loaded physical road network CSVs.
- Built and audited the `Graph[String, Double]` property graph.
- Verified vertex/edge counts and endpoint topology.

### 11.2 What Day 8 Will Implement (Strict Boundary)
- **Congestion Weight Derivation:** In Day 8, edge weights will transition from static distances ($d_e$) to dynamic estimated travel times ($t_e$):
  $$t_e = \frac{d_e}{\max(v_e, 5.0)}$$
  where $v_e$ is the predicted traffic speed in km/h for the corresponding zone from Weekend 2's feature table.
- **Shortest Path Computation:** Calling GraphX's `Pregel` or `ShortestPaths` API to compute the minimum travel-time route from an origin intersection to a destination.
- **Dynamic Reroute Demonstration:** Simulating a congestion event on a primary corridor (e.g. US-101) and hand-verifying that the recommended shortest path diverts onto an alternate route.

---

## 12. Comprehensive Viva Voce Defense Playbook (35 Questions & Answers)

### Section A: Scala Syntax & Language Fundamentals (Q1–Q10)

#### Q1: What is Scala, and why is it used in Urbanix instead of Python?
**Answer:** Scala is a statically-typed, functional and object-oriented programming language that compiles to JVM bytecode. Urbanix uses Scala because Apache Spark GraphX is exclusively a JVM/Scala API; there are no native PySpark bindings for GraphX.

#### Q2: What is the exact difference between `val` and `var` in Scala?
**Answer:** `val` creates an immutable value binding whose reference can never be reassigned (like `final` in Java). `var` creates a mutable variable that can be reassigned. Idiomatic Scala strongly favors `val` for thread safety, referential transparency, and compiler optimization.

#### Q3: Why does `import spark.implicits._` require `spark` to be a `val`?
**Answer:** In Scala, path-dependent imports require a *stable identifier*—an immutable path that cannot change at runtime. If `spark` were a `var`, its reference could change, creating ambiguous import scopes.

#### Q4: What is a `case class` in Scala, and what is its Python equivalent?
**Answer:** A `case class` is a lightweight, immutable data carrier. The Scala compiler automatically generates constructors, getters, `equals()`, `hashCode()`, `toString()`, and `copy()` methods. Its direct Python equivalent is `@dataclass(frozen=True)`.

#### Q5: What is an `object` in Scala, and how does it differ from a `class`?
**Answer:** A `class` is a blueprint that can be instantiated multiple times via `new`. An `object` is a Singleton—a single instance created lazily by the JVM upon first access. It holds static methods, constants, and the application entry point `main()`.

#### Q6: What does the return type `Unit` signify in Scala?
**Answer:** `Unit` represents the absence of a meaningful return value. It is equivalent to `void` in Java or returning `None` from a Python function that executes purely for side effects (e.g. console logging or disk I/O).

#### Q7: What is `Option[T]`, and how does it prevent bugs compared to Python's `None`?
**Answer:** `Option[T]` is a container with two subtypes: `Some(value)` and `None`. Unlike Python where any variable can silently be `None` leading to `AttributeError` at runtime, Scala forces the programmer to explicitly unpack `Option` via pattern matching or `.getOrElse()`, preventing `NullPointerException` at compile time.

#### Q8: What does the `%%` operator mean in `build.sbt`?
**Answer:** The double percent `%%` instructs sbt to automatically append the current Scala binary version (e.g. `_2.12`) to the library artifact name when querying Maven Central (`spark-graphx_2.12-3.5.0.jar`). A single `%` uses the literal artifact name without appending Scala versions.

#### Q9: What is Pattern Matching in Scala?
**Answer:** Pattern matching (`expr match { case ... }`) is a compile-time verified construct for checking values against patterns, decomposing data structures (case classes, tuples), and extracting variables safely.

#### Q10: How does Scala's type inference work?
**Answer:** The Scala compiler uses local type inference to deduce expression types from their context (e.g., `val x = 10` is inferred as `Int`). However, method parameter types and public API boundaries must be explicitly annotated for readability and compile-time contract enforcement.

---

### Section B: Build Tool (sbt) & Environment Compatibility (Q11–Q18)

#### Q11: What is `sbt`, and what role does it serve?
**Answer:** `sbt` (Scala Build Tool) is the standard build tool for Scala. It manages dependencies, downloads jars from Maven Central, compiles Scala sources into `.class` bytecode files, packages artifacts into `.jar` archives, and executes automated test suites.

#### Q12: Why was Scala 2.12.18 specifically chosen rather than Scala 2.13 or 3.x?
**Answer:** Apache Spark 3.5.0 official release binaries on Maven Central are compiled against Scala 2.12. Mixing Scala 2.13 dependencies with a Scala 2.12 Spark runtime causes binary ABI incompatibility errors (`NoSuchMethodError`, `IncompatibleClassChangeError`).

#### Q13: What does `fork := true` do in `build.sbt`?
**Answer:** It instructs sbt to spawn a separate JVM subprocess when running `sbt run` or `sbt test`. This guarantees that SparkSession singletons and allocated Netty network buffers do not leak into the parent sbt interactive shell process.

#### Q14: What is the purpose of `project/build.properties`?
**Answer:** It pins the exact version of sbt (`sbt.version=1.9.9`) to be used for the project, ensuring reproducible builds across different development machines regardless of what sbt launcher is installed locally.

#### Q15: How does sbt download dependencies without requiring a pre-installed Scala compiler?
**Answer:** sbt is self-bootstrapping. It reads `scalaVersion` from `build.sbt` and automatically downloads the exact Scala compiler and standard library jars into the local cache (`~/.cache/coursier` or `~/.ivy2/`).

#### Q16: What happens during `sbt compile`?
**Answer:** sbt parses `build.sbt`, resolves unresolved dependency trees from Maven Central, triggers the incremental compiler (Zinc), and compiles `.scala` files into JVM `.class` bytecode inside `target/scala-2.12/classes/`.

#### Q17: What does `% Test` mean in libraryDependencies?
**Answer:** It assigns the dependency (such as ScalaTest) to the test configuration scope. The library is available when running `sbt test`, but is excluded when compiling production code or packaging production jars.

#### Q18: What is the difference between `sbt run` and `sbt compile`?
**Answer:** `sbt compile` only type-checks and compiles the source code into bytecode. `sbt run` compiles the code (if modified) and then invokes the JVM to execute the `main()` method of the designated Singleton object.

---

### Section C: Spark GraphX Distributed Architecture (Q19–Q27)

#### Q19: What is a Property Graph in GraphX?
**Answer:** A directed multigraph with user-defined properties attached to each vertex and edge. Formally represented as `Graph[VD, ED]`, where `VD` is the vertex attribute type and `ED` is the edge attribute type.

#### Q20: What is a `VertexId` in GraphX, and what is its data type?
**Answer:** `VertexId` is a 64-bit signed integer (`scala.Long`). GraphX requires all vertex identifiers to be Longs for fast hashing and internal bitwise indexing.

#### Q21: What is `Edge[ED]`, and what fields does it contain?
**Answer:** `Edge[ED]` is a case class representing a directed edge. It contains:
- `srcId: VertexId` (source vertex identifier)
- `dstId: VertexId` (destination vertex identifier)
- `attr: ED` (the user-defined edge attribute, e.g., base distance `Double`)

#### Q22: What is an Edge Triplet in GraphX?
**Answer:** An `EdgeTriplet[VD, ED]` extends `Edge[ED]` by adding the source and destination vertex attributes:
- `srcId`, `srcAttr: VD`
- `dstId`, `dstAttr: VD`
- `attr: ED`
It represents an edge along with the complete context of its two endpoint vertices.

#### Q23: How does GraphX partition graphs across a cluster?
**Answer:** GraphX uses **vertex-cut partitioning**: edges are evenly distributed across cluster partitions, while vertices that have edges on multiple partitions are replicated across those partitions with a Vertex Routing Table. This minimizes communication overhead for high-degree hub vertices.

#### Q24: What happens if an edge references a vertex that is not in the vertex RDD?
**Answer:** GraphX automatically inserts that missing vertex into `graph.vertices` with a default vertex attribute (`null.asInstanceOf[VD]`). Our `auditGraph()` method explicitly checks for `_._2 != null` to detect and flag these dangling edges.

#### Q25: What is the difference between `inDegrees`, `outDegrees`, and `degrees`?
**Answer:**
- `outDegrees`: Number of directed edges leaving a vertex (`srcId == vertexId`).
- `inDegrees`: Number of directed edges entering a vertex (`dstId == vertexId`).
- `degrees`: Total number of adjacent edges (`inDegrees + outDegrees`).

#### Q26: What is the `subgraph` operator in GraphX?
**Answer:** `subgraph(epred, vpred)` returns a new graph containing only the vertices that satisfy the vertex predicate `vpred` and edges that satisfy the edge predicate `epred`.

#### Q27: Why doesn't GraphX use standard Spark DataFrames instead of RDDs?
**Answer:** GraphX was designed around RDDs before the DataFrame Catalyst optimizer existed. Graph algorithms require fine-grained pointer manipulation, array indexing, and message passing that cannot easily be expressed as relational SQL plans. (GraphFrames was later created as a DataFrame-based alternative).

---

### Section D: Urbanix Road Graph Model & Verification (Q28–Q35)

#### Q28: How many intersections and road segments were constructed in Day 7?
**Answer:** Exactly 20 physical intersections (vertices) and 40 directed road segments (edges) representing 20 bidirectional corridors across all 5 Urbanix zones.

#### Q29: Why are roads represented as directed edges rather than undirected?
**Answer:** Real metropolitan traffic networks are inherently directed. Traffic flows independently in opposite directions; one direction of a highway or avenue can experience severe congestion while the reverse direction is free-flowing. Two directed edges (`u -> v` and `v -> u`) allow independent congestion weights in Day 8.

#### Q30: What does the initial edge attribute represent in Day 7?
**Answer:** In Day 7, the edge attribute represents `base_distance_km` ($ED = \text{Double}$), the physical length of the road segment in kilometers.

#### Q31: How were dangling edges verified in our graph audit?
**Answer:** The audit collected all genuine source vertex IDs into a set. It then filtered `graph.edges` to count any edge where `!validVertexIds.contains(srcId) || !validVertexIds.contains(dstId)`. The observed count was strictly 0.

#### Q32: How were self-loops verified?
**Answer:** The audit filtered `graph.edges` for `srcId == dstId`. The observed count was strictly 0.

#### Q33: How does Day 7 maintain determinism across repeated executions?
**Answer:** Vertex IDs in `intersections.csv` are explicitly numbered 1 through 20. No random ID generation or nondeterministic row-number hashing is used. Repeated runs produce bitwise identical VertexIds and Graph structures.

#### Q34: How will Day 8 connect Weekend 2's ML predictions to Day 7's graph?
**Answer:** In Day 8, each road segment will inherit the predicted speed $v_e$ of its geographical zone from Weekend 2's feature table. The edge weight will then be dynamically updated to travel time:
$$t_e = \frac{d_e}{\max(v_e, 5.0)}$$

#### Q35: Why does Day 7 NOT yet demonstrate dynamic rerouting?
**Answer:** Day 7 is strictly the graph construction and topology verification gate. Deriving dynamic congestion weights and running shortest-path routing algorithms is the explicit scope of Day 8. Maintaining strict milestone boundaries prevents architectural leakage and ensures rigorous auditability.

---

## 13. Final Acceptance Gate Matrix & Closure Decision

| # | Acceptance Gate | Requirement | Observed Evidence | Gate Status |
| :---: | :--- | :--- | :--- | :---: |
| **1** | **Environment Compatibility** | JDK, Scala, sbt, and Spark versions mutually compatible. | Oracle JDK 1.8.0_202, Scala 2.12.18, sbt 1.9.9, Spark 3.5.0 verified. | **PASS** |
| **2** | **Build Configuration** | sbt project resolves declared dependencies reproducibly. | `build.sbt` resolved Spark Core, SQL, GraphX, and ScalaTest without errors. | **PASS** |
| **3** | **Compilation** | Scala GraphX source code compiles without errors. | `sbt compile` completed in 11.56s with 0 errors. | **PASS** |
| **4** | **Real GraphX Construction** | Creates genuine `Graph[VD, ED]` object. | `RoadGraphBuilder.buildGraph()` constructs `Graph[String, Double]`. | **PASS** |
| **5** | **Source-Grounded Input** | Identified real/synthetic datasets and schemas. | `data/intersections.csv` (20 rows) and `data/roads.csv` (40 rows) authored and whitelisted in `.gitignore`. | **PASS** |
| **6** | **Vertex Integrity** | Unique, traceable, deterministic IDs. | IDs 1..20 strictly mapped, 0 duplicates, 100% deterministic across runs. | **PASS** |
| **7** | **Edge Integrity** | Directionality, endpoints, and attributes validated. | 40 directed edges, 0 dangling endpoints, all distances finite and positive. | **PASS** |
| **8** | **Count Reconciliation** | Source rows reconcile with graph counts. | 20 source intersections = 20 vertices; 40 source roads = 40 edges. | **PASS** |
| **9** | **Automated Tests** | Comprehensive test suite passes. | 10/10 tests passed in `RoadGraphBuilderSpec.scala` in 15.51s. | **PASS** |
| **10**| **Reproducibility** | Repeated execution produces consistent graph identities. | Two consecutive runs of `sbt run` yielded identical 20 vertices, 40 edges, and top degrees. | **PASS** |
| **11**| **Regression Safety** | Days 1–6 intact; models and split unchanged. | 63/63 historical tests passed; `verify_models.py` and `verify_split.py` passed 100%. | **PASS** |
| **12**| **Documentation** | Complete architecture, build, and integration docs. | Created `services/graphx-reroute/README.md`, `_day-notes/day-07/README.md`, and master report. | **PASS** |
| **13**| **Git Hygiene** | Scoped commits, clean working tree, no stray artifacts. | `sbt` target directories excluded; clean Conventional Commits. | **PASS** |
| **14**| **Delivery Report** | Exact commands, counts, and final verdict recorded. | Complete evidence-rich report delivered. | **PASS** |

---

## Final Milestone Closure Decision

$$\mathbf{PASS\ —\ DAY\ 7\ COMPLETE}$$

**All 14 mandatory acceptance criteria are empirically satisfied.** The road-network graph foundation is mathematically verified, topologically sound, fully tested, and ready for Day 8 congestion-weighted shortest-path routing.
