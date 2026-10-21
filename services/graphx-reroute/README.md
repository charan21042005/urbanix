# Urbanix — GraphX Road Network Rerouting Engine (Day 7)

Welcome to the **Urbanix GraphX Road Network Engine**, the distributed graph computation module of the Urbanix Smart City IoT Traffic and Environmental Risk Forecasting Platform.

This module marks the start of **Weekend 3 (GraphX Rerouting)**. It establishes a dedicated Scala and sbt project that interfaces directly with **Apache Spark GraphX** to construct, validate, and audit a distributed property graph of the metropolitan road network across all five Urbanix sensor zones.

---

## 1. Why Scala & GraphX?

### 1.1 The Need for GraphX
Urbanix predicts short-term traffic speeds and environmental risks per zone using Spark Streaming and Spark MLlib. However, predicting congestion is only half the solution. A smart city platform must actively **reroute vehicles around anticipated congestion bottlenecks**. 

Standard tabular or relational processing engines (like SQL or standard PySpark DataFrames) represent road networks as flat lists of edges. Performing shortest-path calculations (such as Dijkstra or Pregel message passing) on flat DataFrames requires expensive, iterative Cartesian joins across the entire dataset on every graph step. 

**Apache Spark GraphX** is Spark's specialized API for graphs and graph-parallel computation. It extends the Spark RDD abstraction by introducing the **Resilient Distributed Property Graph**: a directed multigraph with user-defined properties attached to each vertex and edge. GraphX optimizes iterative algorithms using:
1. **Vertex Routing Tables**: Routing tables colocated with edge partitions to eliminate unnecessary network shuffles.
2. **Pregel API**: A bulk-synchronous parallel (BSP) message-passing abstraction where vertices receive messages from incoming edges, update their state, and send messages along outgoing edges in parallel supersteps.

### 1.2 Why Scala?
Unlike Spark SQL, Structured Streaming, and MLlib (which provide complete Python bindings via Py4J), **Apache Spark GraphX is exclusively a JVM/Scala API**. There is no official PySpark wrapper for GraphX. While Python third-party wrappers like GraphFrames exist, implementing the routing engine in native Scala provides:
- Direct, unmediated access to Spark's internal GraphX operators (`triplets`, `subgraph`, `aggregateMessages`, and `Pregel`).
- Maximum distributed execution performance with zero Py4J serialization overhead.
- True type safety at compile time for vertices (`VertexId`, `VD`) and edges (`Edge[ED]`).

---

## 2. Architecture & Property Graph Model

The road network is modeled as a **Directed Property Graph** $G = (V, E)$:

$$\text{Graph}[VD, ED]$$

Where:
- **$V$ (Vertices)**: Physical road intersections. Each vertex is an entry in a `VertexRDD[(VertexId, VD)]`:
  - `VertexId`: A unique 64-bit Long identifier.
  - `VD`: Vertex Data, storing the human-readable intersection name (`String`) and its associated zone.
- **$E$ (Edges)**: Directed road segments. Each edge is an entry in an `EdgeRDD[Edge[ED]]`:
  - `srcId`: 64-bit Long identifier of the source intersection.
  - `dstId`: 64-bit Long identifier of the destination intersection.
  - `attr` ($ED$): Edge Data, initialized with `base_distance_km` (`Double`), the physical road distance in kilometers.

```
[Vertex 1: Grand & 1st] -- Edge (base_dist = 1.2 km) --> [Vertex 2: Broadway & 7th]
                        <-- Edge (base_dist = 1.2 km) --
```

### 2.1 Metropolitan Road Topology
The network spans **20 intersections** and **40 directed edges** (20 bidirectional road corridors) covering all five Urbanix operational zones:
- **`zone_1` (Downtown Core / CBD)**: 4 nodes (1, 2, 3, 4) — high density arterial grid.
- **`zone_2` (San Fernando Valley)**: 4 nodes (5, 6, 7, 8) — suburban grid connected via US-101.
- **`zone_3` (San Gabriel Valley / Pasadena)**: 4 nodes (9, 10, 11, 12) — foothill arterials connected via I-210/I-110.
- **`zone_4` (South Central Corridor / Pico Rivera)**: 4 nodes (13, 14, 15, 16) — transit spine connected via I-110/I-605.
- **`zone_5` (Harbor / Long Beach Port)**: 4 nodes (17, 18, 19, 20) — port logistics corridor connected via I-710.

---

## 3. Directory Layout & Build Configuration

```
services/graphx-reroute/
├── build.sbt                          # Project dependencies & Scala version definition
├── project/
│   └── build.properties               # Pins sbt version (1.9.9)
├── sbt.bat                            # Windows launcher wrapper for local sbt execution
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── RoadGraphBuilder.scala # Graph construction, loader, & audit logic
│   └── test/
│       └── scala/
│           └── RoadGraphBuilderSpec.scala # 10-test automated verification suite
└── README.md                          # Architecture and operational documentation
```

### 3.1 Pinned Dependencies & Binary Compatibility
In `build.sbt`:
```scala
name := "urbanix-graphx-reroute"
version := "0.1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"   % "3.5.0",
  "org.apache.spark" %% "spark-sql"    % "3.5.0",
  "org.apache.spark" %% "spark-graphx" % "3.5.0",
  "org.scalatest"    %% "scalatest"    % "3.2.18" % Test
)
```

> [!IMPORTANT]
> **Scala 2.12 Compatibility**: Apache Spark 3.5.0 pre-built binaries on Maven Central are compiled against **Scala 2.12**. Attempting to pair Spark 3.5 with Scala 2.13 artifacts results in fatal `IncompatibleClassChangeError` and `NoSuchMethodError` exceptions. Scala 2.12.18 is strictly enforced.

---

## 4. Execution Guide (Windows / PowerShell)

All commands can be executed directly from PowerShell within `services/graphx-reroute/` using either the system `sbt` command or the provided `.\sbt.bat` wrapper.

### 4.1 Compile the Project
Verifies syntax, resolves dependencies, and compiles Scala sources:
```powershell
cd services/graphx-reroute
.\sbt.bat --batch compile
```

### 4.2 Run the Automated Test Suite (10 Tests)
Executes all unit tests in `RoadGraphBuilderSpec.scala`:
```powershell
.\sbt.bat --batch test
```
The test suite covers:
1. Basic graph construction against fixtures.
2. Deterministic vertex ID mapping across repeated runs.
3. Duplicate intersection identifier detection.
4. Missing road endpoint (dangling edge) detection.
5. Invalid/non-positive edge attribute handling.
6. Edge directionality preservation (no phantom reverse edges).
7. Self-loop detection (`srcId == dstId`).
8. Empty dataset edge cases.
9. Source-count reconciliation.
10. Native GraphX operations (`triplets`, `inDegrees`, `outDegrees`, `subgraph`).

### 4.3 Run the Road Graph Builder
Executes `RoadGraphBuilder.main()`, constructing the graph from `data/intersections.csv` and `data/roads.csv`:
```powershell
.\sbt.bat --batch run
```
Expected output:
```text
======================================================================
URBANIX DAY 7: SPARK GRAPHX ROAD-GRAPH CONSTRUCTION & AUDIT
======================================================================
[*] Loading Intersections Source : .../data/intersections.csv
[*] Loading Roads Source         : .../data/roads.csv
[*] Source CSV Rows: 20 intersections, 40 road segments
----------------------------------------------------------------------
GRAPH INTEGRITY & TOPOLOGY AUDIT REPORT
----------------------------------------------------------------------
[*] Source Intersections (Vertices) : 20
[*] Accepted Graph Vertices         : 20
[*] Source Road Segments (Edges)    : 40
[*] Accepted Graph Edges            : 40
[*] Dangling Edges (Missing Endpts) : 0
[*] Self-Loop Edges                 : 0
======================================================================
FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY
======================================================================
```

---

## 5. Day 8 Integration Boundary

Day 7 establishes the **validated static graph topology**. 
In **Day 8 (Congestion-Weighted Shortest Path)**:
1. Each road segment's static weight (`base_distance_km`) will be dynamically adjusted using the live traffic speed predictions generated by the Day 5/6 MLlib models:
   $$\text{travel\_time\_hours} = \frac{\text{base\_distance\_km}}{\max(\text{predicted\_speed\_kmh}, 5.0)}$$
2. GraphX's `Pregel` / `ShortestPaths` API will compute dynamic shortest travel-time routes from origin to destination intersections, demonstrating real-time rerouting as congestion spikes.
