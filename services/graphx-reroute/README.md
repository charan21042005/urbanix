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

### 2.1 Metropolitan Road Topology & Provenance
The road network is a **synthetic demonstration topology** designed specifically to model inter-zone highway and arterial connectivity across the five Urbanix operational zones. It provides illustrative coordinates and road distances for distributed graph algorithm verification:
- **`zone_1` (Downtown Core / CBD)**: 4 nodes (1, 2, 3, 4) — high density arterial grid.
- **`zone_2` (San Fernando Valley)**: 4 nodes (5, 6, 7, 8) — suburban grid connected via US-101.
- **`zone_3` (San Gabriel Valley / Pasadena)**: 4 nodes (9, 10, 11, 12) — foothill arterials connected via I-210/I-110.
- **`zone_4` (South Central Corridor / Pico Rivera)**: 4 nodes (13, 14, 15, 16) — transit spine connected via I-110/I-605.
- **`zone_5` (Harbor / Long Beach Port)**: 4 nodes (17, 18, 19, 20) — port logistics corridor connected via I-710.

Total: **20 intersections** and **40 directed edges** (20 bidirectional road corridors). Because every road corridor has directed segments in both directions ($u \to v$ and $v \to u$), the network has 100% reciprocity / symmetry. Consequently, the weakly connected component count ($k=1$) is mathematically identical to the strongly connected component count ($k=1$).

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
│   │       └── RoadGraphBuilder.scala # Two-phase validation, GraphX construction & audit logic
│   └── test/
│       └── scala/
│           └── RoadGraphBuilderSpec.scala # 13-test automated verification suite
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

### 4.2 Run the Automated Test Suite (13 Tests)
Executes all unit tests in `RoadGraphBuilderSpec.scala`:
```powershell
.\sbt.bat --batch test
```
The test suite covers:
1. Basic graph construction against fixtures.
2. Deterministic vertex ID mapping across repeated runs.
3. Production validator detection of duplicate intersection IDs.
4. Production graph audit detection of dangling edges and GraphX phantom vertices.
5. Production validator detection of invalid (negative, zero, NaN, infinite) road distances.
6. Edge directionality preservation (no phantom reverse edges fabricated).
7. Production validator and audit detection of self-loops (`src_id == dst_id`).
8. Empty dataset edge cases (0 vertices, 0 edges).
9. Source-count reconciliation against graph counts.
10. Native GraphX operations (`triplets`, `inDegrees`, `outDegrees`, `subgraph`).
11. Production `validateAndBuildGraph` strict fail-fast policy (`IllegalArgumentException`).
12. Production validator distinction between accidental duplicate records and legitimate reverse edges.
13. Verified topology audit on genuine checked-in CSVs (20 vertices, 40 edges, 1 component, 100% reciprocity).

### 4.3 Run the Road Graph Builder
Executes `RoadGraphBuilder.main()`, constructing the graph from `data/intersections.csv` and `data/roads.csv`:
```powershell
.\sbt.bat --batch run
```
Expected output:
```text
======================================================================
URBANIX DAY 7: SPARK GRAPHX ROAD-GRAPH CONSTRUCTION & RIGOROUS AUDIT
======================================================================
[*] Loading Intersections Source : .../data/intersections.csv
[*] Loading Roads Source         : .../data/roads.csv
----------------------------------------------------------------------
PHASE 1: PRE-CONSTRUCTION SOURCE DATA VALIDATION
----------------------------------------------------------------------
[*] Total Source Intersection Rows : 20
[*] Distinct Intersection IDs      : 20
[*] Duplicate Intersection IDs     : 0
[*] Null or Invalid Node IDs       : 0
[*] Blank Intersection Names       : 0
[*] Total Source Road Segments     : 40
[*] Exact Duplicate Road Records   : 0
[*] Invalid Endpoint Records       : 0
[*] Invalid Distance Records       : 0
[*] Self-Loop Records              : 0
[*] Source Data Validation Status  : [PASS] VALID
----------------------------------------------------------------------
PHASE 2: GRAPHX PROPERTY GRAPH CONSTRUCTION & AUDIT
----------------------------------------------------------------------
[*] Accepted Graph Vertices        : 20
[*] Accepted Graph Edges           : 40
[*] Dangling Edges (Missing Endpts): 0
[*] Phantom GraphX Vertices        : 0
[*] Self-Loop Edges                : 0
[*] Weakly Connected Components    : 1
[*] Directed Symmetry (Reciprocity): 100% Symmetric (Bidirectional Links)
======================================================================
FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY
Road network graph is verified and ready for Day 8 shortest-path routing.
======================================================================
```

---

## 5. Day 8 Architecture: Congestion-Weighted Pregel Routing

On **Day 8**, we transformed the static road network into an active, congestion-weighted shortest-path routing engine.

### 5.1 Travel-Time Edge Cost Modeling (`CongestionWeight.scala`)
Routing minimizes travel time rather than physical distance:
$$\text{travel\_time\_hours} = \frac{\text{base\_distance\_km}}{\max(\text{speed\_kmph}, 1.0)}$$
- **Units:** Hours (strictly positive and finite).
- **Gridlock Floor:** Near-zero speeds are defensively clamped to $1.0\text{ km/h}$ to prevent division by zero while penalizing gridlocked corridors.

### 5.2 Why Built-In `ShortestPaths` Fails vs. GraphX Pregel
`org.apache.spark.graphx.lib.ShortestPaths` only counts **unweighted hops** (number of turns/edges), completely ignoring edge weights. Urbanix implements custom weighted SSSP using **GraphX Pregel** (`ShortestPathPregel.scala`):
- **Vertex Program (`vprog`)**: Updates vertex state with minimum candidate travel time and route path.
- **Send Message (`sendMsg`)**: Relaxes directed edges `src.dist + edge.weight < dst.dist` along outgoing edges.
- **Merge Message (`mergeMsg`)**: Keeps minimum travel time with deterministic lexicographic tie-breaking.

### 5.3 Execution & Verification Commands
```powershell
cd services\graphx-reroute

# 1. Run full 31-test Scala suite (Day 7 audit + Day 8 Pregel routing):
.\sbt.bat --batch test

# 2. Run multi-stage routing console application:
.\sbt.bat --batch "runMain urbanix.graphx.FullGraphRoutingApp"
```

---

## 6. Day 9 Integration Boundary & Contract

- **Day 8 Status:** SSSP routing algorithm and travel-time calculator are fully verified using Section 3.1 4-node oracle, dynamic reroute proof, and 20-node demonstration graph using a $50\text{ km/h}$ placeholder baseline speed.
- **Day 9 Mission:** Wire genuine Day 6 MLlib speed predictions into road edge weights, recompute optimal routes dynamically as streaming forecasts update, and persist route recommendations for the geospatial dashboard.
- **Strict Boundary:** Live MLlib-to-GraphX prediction wiring remains strictly Day 9 work.
