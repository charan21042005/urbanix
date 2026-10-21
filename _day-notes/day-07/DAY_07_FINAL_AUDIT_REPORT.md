# Urbanix Day 7 — Final Independent Engineering Audit, Targeted Corrections and Closure Report

- **Repository:** `https://github.com/charan21042005/urbanix`
- **Branch:** `main`
- **Initial Baseline Commit:** `27f42081545a3e9b849b354b3e1dede03071c75e`
- **Module Audited:** `services/graphx-reroute/`
- **Audit Date:** October 21, 2026
- **Auditor:** Antigravity Autonomous Systems Engineering (Pair Programming Independent Auditor)
- **Final Verdict:** **PASS — DAY 7 COMPLETE**

---

## 1. Executive Summary & Purpose

This audit serves as the final engineering gate for **Urbanix Day 7 (Weekend 3: GraphX Road-Graph Construction & Integrity Auditing)**. Day 7 establishes a validated, distributed road network property graph in **Apache Spark GraphX** with Scala and sbt, forming the foundational graph topology required for Day 8 dynamic shortest-path rerouting.

In accordance with strict closure protocols:
1. Existing work from Days 1–6 (models, checkpoint artifacts, streaming pipelines, chronologically split Parquet datasets) was verified and preserved without regression.
2. The graph-validation gaps identified in the Day 7 initial delivery were independently analyzed, confirmed, and resolved via production code enhancements.
3. All tests were executed against production validation methods rather than localized test filter logic.
4. The genuine checked-in CSV topologies (`data/intersections.csv` and `data/roads.csv`) were audited with exact source-to-graph reconciliation.
5. The full multi-language regression suite across Days 2–7 (76 automated tests: 63 Python + 13 Scala) was executed and confirmed passing.

---

## 2. Environment, Toolchain & Runtime Baseline

An empirical audit of the underlying runtime environment was conducted to prevent binary incompatibility:

| Component | Verified Specification | Source / Command Output | Operational Rationale |
| :--- | :--- | :--- | :--- |
| **Operating System** | Windows 11 Pro (build 10.0.26100) | `cmd /c ver` | Host OS for development & local Spark deployment |
| **Java Virtual Machine** | Oracle JDK 1.8.0_202 (64-bit) | `java -version` | Certified LTS Java runtime for Apache Spark 3.5.0 JVM compatibility |
| **Scala Version** | 2.12.18 | `build.sbt: scalaVersion := "2.12.18"` | Strictly required by Apache Spark 3.5.0 pre-built binaries on Maven |
| **sbt Build Tool** | 1.9.9 | `project/build.properties: sbt.version=1.9.9` | Official Scala Build Tool wrapper (`sbt.bat`) |
| **Spark Engine** | Apache Spark 3.5.0 (Maven dependencies) | `org.apache.spark:spark-core_2.12:3.5.0`<br>`org.apache.spark:spark-sql_2.12:3.5.0`<br>`org.apache.spark:spark-graphx_2.12:3.5.0` | Standalone JVM dependencies downloaded and cached by sbt |
| **Local Spark Warning** | Apache Spark 3.5.9 on Scala 2.13 in `C:\spark\` | Environmental inspection | The local CLI Spark installation is built for Scala 2.13. **Do not execute GraphX jobs against this local distribution**; all execution is isolated to sbt using Scala 2.12.18. |
| **Test Framework** | ScalaTest 3.2.18 (`AnyFunSuite`, `BeforeAndAfterAll`) | `org.scalatest:scalatest_2.12:3.2.18` | Standard Scala test runner integrated with sbt test task |

---

## 3. Files Inspected & Modified

### 3.1 Inspected Files
- `services/graphx-reroute/build.sbt`: Dependency specifications and compiler options.
- `services/graphx-reroute/src/main/scala/RoadGraphBuilder.scala`: Graph construction and audit implementations.
- `services/graphx-reroute/src/test/scala/RoadGraphBuilderSpec.scala`: Automated unit tests.
- `services/graphx-reroute/README.md`: Module architecture, setup guide, and operational boundaries.
- `data/intersections.csv`: 20-node intersection master dataset.
- `data/roads.csv`: 40-segment directed road master dataset.
- `_day-notes/day-07/DAY_07_GRAPHX_CONSTRUCTION_REPORT.md`: Initial construction report.
- `_day-notes/day-07/README.md`: Day 7 overview index.

### 3.2 Modified Files
1. `services/graphx-reroute/src/main/scala/RoadGraphBuilder.scala`:
   - Added `case class SourceValidationReport` to capture pre-construction data quality metrics.
   - Expanded `case class GraphSummary` to include phantom vertex detection, reciprocity, and component count.
   - Implemented `validateSourceData(intersectionsDF, roadsDF)` for independent source auditing.
   - Enhanced `auditGraph(graph, intersectionsDF, roadsDF)` to detect GraphX phantom vertices and evaluate directed symmetry.
   - Implemented `validateAndBuildGraph(spark, intersectionsDF, roadsDF, strict = true)` to provide fail-fast validation for production pipelines.
2. `services/graphx-reroute/src/test/scala/RoadGraphBuilderSpec.scala`:
   - Expanded test suite from 10 to 13 tests.
   - Refactored tests 3, 4, 5, 7, and 9 to invoke production methods directly rather than using inline filter logic.
   - Added Test 11 for strict fail-fast `IllegalArgumentException` handling.
   - Added Test 12 distinguishing accidental duplicate records from valid bidirectional links.
   - Added Test 13 validating genuine checked-in CSV topology, reciprocity, and strong connectivity.
3. `services/graphx-reroute/README.md`:
   - Updated documentation to reflect the 13-test suite, two-phase validation architecture, and synthetic topology provenance.
4. `_day-notes/day-07/README.md`:
   - Updated repository test total (76 tests) and added link to this final audit report.

---

## 4. Confirmed Defects, Root Cause Analysis & Targeted Corrections

### Defect 1: Graph-Inferred Vertex Audit Incompleteness (Circular Audit)
- **Confirmed Defect:** The initial `auditGraph(graph)` implementation extracted vertex IDs solely from the already-constructed GraphX `graph.vertices.map(_._1)`.
- **Root Cause:** In Apache Spark GraphX, if an edge references a vertex ID that was not provided in the vertex RDD, GraphX automatically introduces a "phantom" vertex with a default attribute (`null.asInstanceOf[VD]`). As a consequence, inferring source vertices post-construction masks missing vertex definitions and circularizes the verification of dangling endpoints.
- **Correction:** Implemented independent pre-construction source validation (`validateSourceData`) and an expanded `auditGraph(graph, intersectionsDF, roadsDF)`. The audit cross-references graph vertices against the source DataFrames:
  ```scala
  val validSourceIds = intersectionsDF.select(col("node_id").cast(LongType)).rdd.map(_.getLong(0)).collect().toSet
  val phantomVertices = graph.vertices.filter { case (id, attr) =>
    attr == null || !validSourceIds.contains(id)
  }.count()
  ```

### Defect 2: Conflation of Bidirectional Edges with Duplicate Records
- **Confirmed Defect:** The initial design lacked an explicit mechanism to distinguish legitimate reverse-direction edges `(u, v)` and `(v, u)` from accidental duplicate records `(u, v)` and `(u, v)`.
- **Root Cause:** A bidirectional road segment in a directed graph consists of two distinct edges: $u \to v$ and $v \to u$. If duplicate detection only tracks unordered pairs $(u, v) \equiv (v, u)$, valid bidirectional links are falsely flagged as duplicates.
- **Correction:** Implemented exact directed pair deduplication in `validateSourceData`:
  ```scala
  val exactDuplicateRoads = roadsDF.groupBy("src_id", "dst_id").count().filter(col("count") > 1).count()
  ```
  Legitimate reverse edges are preserved, while identical duplicate rows are flagged and rejected in strict mode.

### Defect 3: Non-Production Test Verification (Simulated Test Filters)
- **Confirmed Defect:** Several tests in `RoadGraphBuilderSpec.scala` reimplemented filter operations inside the test body instead of testing the actual production methods in `RoadGraphBuilder`.
- **Root Cause:** The tests were initially written as local assertions on temporary DataFrames, which did not prove that production code properly executed those validation checks.
- **Correction:** Refactored tests to directly invoke `RoadGraphBuilder.validateSourceData`, `RoadGraphBuilder.auditGraph`, and `RoadGraphBuilder.validateAndBuildGraph`. Added targeted tests:
  - Test 3 tests `validateSourceData` on duplicate node IDs.
  - Test 4 tests `auditGraph` on dangling edges and GraphX phantom vertices.
  - Test 5 tests `validateSourceData` on negative, zero, and NaN distances.
  - Test 7 tests `validateSourceData` on self-loops (`src_id == dst_id`).
  - Test 11 tests strict fail-fast exception throwing (`IllegalArgumentException`).
  - Test 12 tests that bidirectional roads are preserved while exact duplicates are flagged.
  - Test 13 tests the genuine checked-in CSV dataset.

### Defect 4: Directed Graph Connectivity Rigor
- **Confirmed Defect:** Earlier documentation stated the directed road graph was connected without explicitly specifying whether this referred to weak or strong connectivity.
- **Root Cause:** `ConnectedComponents.run` in GraphX operates on the underlying undirected graph (weak connectivity). In a general directed graph, weak connectivity does not imply strong connectivity.
- **Correction:** Clarified the mathematical relationship:
  1. The checked-in Urbanix road network has **100% reciprocity** (every edge $u \to v$ has an accompanying edge $v \to u$).
  2. Because the adjacency matrix is symmetric, the directed graph is symmetric.
  3. For symmetric directed graphs, a weakly connected component is mathematically identical to a strongly connected component.
  4. The network has exactly 1 connected component ($k=1$), proving full reachability between all 20 intersections.

---

## 5. Execution Evidence & Console Logs

All commands were executed using PowerShell directly in the local repository.

### 5.1 Scala Compilation (`compile`)
- **Command:** `& "$env:LOCALAPPDATA\Programs\sbt\bin\sbt.bat" --batch compile`
- **Working Directory:** `services/graphx-reroute/`
- **Exit Code:** `0`
- **Output:**
  ```text
  [info] welcome to sbt 1.9.9 (Oracle Corporation Java 1.8.0_202)
  [info] loading project definition from ...\services\graphx-reroute\project
  [info] loading settings for project graphx-reroute from build.sbt ...
  [info] set current project to urbanix-graphx-reroute
  [info] compiling 1 Scala source to ...\services\graphx-reroute\target\scala-2.12\classes ...
  [info] done compiling
  [success] Total time: 8 s, completed 21 Oct, 2026 10:05:08 AM
  ```

### 5.2 Automated Scala Test Suite (`test` - 13 Tests)
- **Command:** `& "$env:LOCALAPPDATA\Programs\sbt\bin\sbt.bat" --batch test`
- **Working Directory:** `services/graphx-reroute/`
- **Exit Code:** `0`
- **Output:**
  ```text
  [info] RoadGraphBuilderSpec:
  [info] - Test 1: Basic graph construction matches fixture vertex and edge counts
  [info] - Test 2: Deterministic vertex mapping produces identical IDs across runs
  [info] - Test 3: Production validator flags duplicate intersection IDs and fails source audit
  [info] - Test 4: Production graph audit flags dangling edges and GraphX phantom vertices
  [info] - Test 5: Production validator flags negative, zero, and NaN road distances
  [info] - Test 6: Directed edges remain strictly directed and reverse edges are not fabricated
  [info] - Test 7: Production validator and audit flag self-loops (src_id == dst_id)
  [info] - Test 8: Empty datasets construct empty graph with 0 vertices and 0 edges
  [info] - Test 9: Source counts reconcile with graph vertices and edges on known fixture
  [info] - Test 10: GraphX operators (triplets, inDegrees, outDegrees, subgraph) execute correctly
  [info] - Test 11: Production validateAndBuildGraph throws IllegalArgumentException in strict mode on invalid data
  [info] - Test 12: Production validator distinguishes accidental duplicates from valid bidirectional links
  [info] - Test 13: Topology audit on genuine checked-in CSVs verifies strong connectivity via 100% symmetry
  [info] Run completed in 22 seconds, 110 milliseconds.
  [info] Total number of tests run: 13
  [info] Suites: completed 1, aborted 0
  [info] Tests: succeeded 13, failed 0, canceled 0, ignored 0, pending 0
  [info] All tests passed.
  [success] Total time: 35 s, completed 21 Oct, 2026 10:05:52 AM
  ```

### 5.3 Production Application Execution (`run`)
- **Command:** `& "$env:LOCALAPPDATA\Programs\sbt\bin\sbt.bat" --batch run`
- **Working Directory:** `services/graphx-reroute/`
- **Exit Code:** `0`
- **Output:**
  ```text
  [info] running (fork) urbanix.graphx.RoadGraphBuilder 
  [info] ======================================================================
  [info] URBANIX DAY 7: SPARK GRAPHX ROAD-GRAPH CONSTRUCTION & RIGOROUS AUDIT
  [info] ======================================================================
  [info] [*] Loading Intersections Source : ../../data/intersections.csv
  [info] [*] Loading Roads Source         : ../../data/roads.csv
  [info] ----------------------------------------------------------------------
  [info] PHASE 1: PRE-CONSTRUCTION SOURCE DATA VALIDATION
  [info] ----------------------------------------------------------------------
  [info] [*] Total Source Intersection Rows : 20
  [info] [*] Distinct Intersection IDs      : 20
  [info] [*] Duplicate Intersection IDs     : 0
  [info] [*] Null or Invalid Node IDs       : 0
  [info] [*] Blank Intersection Names       : 0
  [info] [*] Total Source Road Segments     : 40
  [info] [*] Exact Duplicate Road Records   : 0
  [info] [*] Invalid Endpoint Records       : 0
  [info] [*] Invalid Distance Records       : 0
  [info] [*] Self-Loop Records              : 0
  [info] [*] Source Data Validation Status  : [PASS] VALID
  ----------------------------------------------------------------------
  [info] PHASE 2: GRAPHX PROPERTY GRAPH CONSTRUCTION & AUDIT
  ----------------------------------------------------------------------
  [info] [*] Accepted Graph Vertices        : 20
  [info] [*] Accepted Graph Edges           : 40
  [info] [*] Dangling Edges (Missing Endpts): 0
  [info] [*] Phantom GraphX Vertices        : 0
  [info] [*] Self-Loop Edges                : 0
  [info] [*] Weakly Connected Components    : 1
  [info] [*] Directed Symmetry (Reciprocity): 100% Symmetric (Bidirectional Links)
  [info] [*] Top Out-Degree Intersections:
  [info]     - Vertex ID 14 : out-degree = 3
  [info]     - Vertex ID 1 : out-degree = 3
  [info]     - Vertex ID 9 : out-degree = 3
  [info]     - Vertex ID 18 : out-degree = 3
  [info]     - Vertex ID 5 : out-degree = 3
  ======================================================================
  [info] FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY
  [info] Road network graph is verified and ready for Day 8 shortest-path routing.
  ======================================================================
  [success] Total time: 19 s, completed 21 Oct, 2026 10:06:21 AM
  ```

---

## 6. Comprehensive Graph Audit & Source Reconciliation

The checked-in dataset was reconciled between source CSV records and the GraphX property graph:

| Audit Parameter | Source CSV Measurement | GraphX Graph Measurement | Status | Diagnostic Evaluation |
| :--- | :--- | :--- | :--- | :--- |
| **Intersection Count** | 20 rows | 20 vertices | **PASS** | Exact 1:1 match across all 5 operational zones |
| **Distinct Vertex IDs** | 20 unique IDs (1 to 20) | 20 unique `VertexId`s | **PASS** | No ID collisions, gaps, or type conversion anomalies |
| **Duplicate Vertex IDs** | 0 duplicate IDs | N/A | **PASS** | Source ID set is strictly unique |
| **Null/Blank Node Names** | 0 null/blank | 0 null/blank | **PASS** | All intersection names populated and valid |
| **Road Segment Count** | 40 rows | 40 directed edges | **PASS** | Exact 1:1 match representing 20 bidirectional corridors |
| **Exact Duplicate Roads** | 0 duplicate `(u, v)` | N/A | **PASS** | No redundant duplicate records |
| **Legitimate Reverse Edges** | 20 reverse pairs | 20 reverse pairs | **PASS** | 100% reciprocal bidirectional corridors |
| **Invalid Endpoints** | 0 missing endpoints | 0 dangling edges | **PASS** | All `src_id` and `dst_id` resolve to valid intersections |
| **Phantom Vertices** | 0 phantom nodes | 0 phantom nodes | **PASS** | No dummy vertices created by GraphX for unreferenced IDs |
| **Invalid Distances** | 0 non-positive/NaN/Inf | 0 non-positive/NaN/Inf | **PASS** | All distances are positive finite numbers ($0.8 \le d \le 12.0$ km) |
| **Self-Loops** | 0 self-loops | 0 self-loops | **PASS** | $u \ne v$ strictly holds across all 40 edges |
| **Weakly Connected Components**| N/A | 1 component | **PASS** | Computed via `ConnectedComponents.run(graph)` |
| **Directed Symmetry** | 100% (40/40 edges) | 100% (40/40 edges) | **PASS** | Every directed edge $(u, v)$ has counterpart $(v, u)$ |
| **Strongly Connected Components**| 1 component | 1 component | **PASS** | Mathematical consequence of $k_{\text{weak}}=1$ and 100% reciprocity |

---

## 7. Multi-Day Regression Suite Verification

To confirm that Day 7 work did not degrade any prior deliverables, the historical test suites were executed:

```
Regression Execution Log:
- Day 2 (Kafka ingestion schema & partition tests):
  pytest services/kafka/tests/ -> 19 passed in 0.57s
- Day 3 (Spark streaming deduplication & watermark tests):
  pytest services/streaming/tests/test_streaming_quality.py -> 14 passed in 61.95s
- Day 4 (Feature pipeline & chronological leakage tests):
  pytest services/streaming/tests/test_feature_pipeline.py -> 11 passed in 60.45s
- Day 5 (MLlib model training & per-zone evaluations):
  pytest services/streaming/tests/test_model_training.py -> 9 passed in 81.29s
- Day 6 (Live inference & recovery tests):
  pytest services/streaming/tests/test_streaming_inference.py -> 10 passed in 40.98s
- Model & Split Invariant Scripts:
  python services/streaming/verify_models.py -> ALL 12 CHECKS PASSED
  python services/streaming/verify_split.py -> PASS (100% zero data leakage)
- Day 7 (GraphX Construction & Integrity Suite):
  sbt --batch test -> 13 passed in 35s
```

**Total Active Test Count Across Repository:** **76 Passing Automated Tests** (63 Python + 13 Scala).

---

## 8. Architectural Boundaries & Day 8 Distinction

Day 7 strictly limits its scope to static graph construction, schema validation, and topology auditing. The boundary between Day 7 and Day 8 is formally established:

1. **Entity Distinction:**
   - **Intersection IDs (Long 1 to 20):** Discrete graph nodes representing physical intersections and highway interchanges.
   - **Sensor Zone IDs (`zone_1` to `zone_5`):** Geographic administrative partitions aggregating streaming sensor feeds and ML forecasts.
2. **Weight Distinction:**
   - **Base Distance (`base_distance_km`):** Static spatial property of the road segment in kilometers.
   - **Dynamic Travel Time (`travel_time_hours`):** Computed in Day 8 by combining `base_distance_km` with real-time predicted speed:
     $$\text{travel\_time\_hours} = \frac{\text{base\_distance\_km}}{\max(\text{predicted\_speed\_kmh}, 5.0)}$$
3. **Data Provenance Limitation:**
   - The road network defined in `data/intersections.csv` and `data/roads.csv` is a **synthetic demonstration topology** designed to validate distributed GraphX algorithms and demonstrate inter-zone connectivity. It must not be characterized as authoritative GIS municipal survey data.
4. **Scope Limitation:**
   - No dynamic shortest-path computations (Dijkstra, Pregel, Bellman-Ford) were implemented in Day 7. Day 8 will consume the verified Day 7 graph foundation to implement congestion-weighted routing.

---

## 9. Day 7 Acceptance Gate Matrix

| Acceptance Gate | Verification Criteria | Status | Verified Evidence |
| :--- | :--- | :--- | :--- |
| **Gate 1: Repository Baseline** | Branch clean, commit history intact, no discarded user edits | **PASS** | Starting commit `27f4208`, verified clean working tree |
| **Gate 2: Toolchain Isolation** | Independent sbt module running Scala 2.12.18 on Java 1.8 | **PASS** | Successful compilation without local Scala 2.13 interference |
| **Gate 3: Pre-Construction Source Audit** | Independent validation of intersections & roads DataFrames | **PASS** | `validateSourceData` returns `VALID`, reports all 10 source metrics |
| **Gate 4: Duplicate ID Protection** | Detection and rejection of duplicate intersection IDs | **PASS** | Unit Test 3 verifies duplicate ID detection and audit failure |
| **Gate 5: Phantom Vertex & Dangling Edge Protection** | GraphX phantom vertices flagged and dangling edges isolated | **PASS** | Unit Test 4 verifies phantom vertex and dangling edge detection |
| **Gate 6: Edge Distance Integrity** | Validation of positive, finite, non-zero edge distances | **PASS** | Unit Test 5 verifies rejection of negative, zero, NaN, infinite distances |
| **Gate 7: Directionality & Reciprocity** | Strict directed edge handling without phantom reverse fabrication | **PASS** | Unit Tests 6 & 12 verify directionality and legitimate reciprocity |
| **Gate 8: Self-Loop Detection** | Flagging and isolation of self-referential edges (`src == dst`) | **PASS** | Unit Test 7 verifies self-loop detection |
| **Gate 9: Strict Mode Fail-Fast** | `validateAndBuildGraph` throws exception on invalid data | **PASS** | Unit Test 11 verifies `IllegalArgumentException` in strict mode |
| **Gate 10: Topology Connectivity** | Component count and strong connectivity proven rigorously | **PASS** | Unit Test 13 verifies 1 component and 100% reciprocity |
| **Gate 11: Production Main Run** | Clean execution of `RoadGraphBuilder.main` on checked-in CSVs | **PASS** | `sbt run` exits with code 0, 20 vertices, 40 edges, 0 errors |
| **Gate 12: Spark Clean Shutdown** | Spark context terminates cleanly without leaked background threads | **PASS** | `spark.stop()` cleanly terminates driver in both app and test suite |
| **Gate 13: Zero Regression** | Days 2–6 regression suites and model invariants remain green | **PASS** | 63 Python tests pass; 12 model checks pass; split invariant holds |
| **Gate 14: Day 8 Scope Boundary** | Static topology delivered without pre-implementing Day 8 routing | **PASS** | Zero routing code implemented; clear architecture boundaries documented |

---

## 10. Final Closure Verdict

```
========================================================================================
FINAL AUDIT VERDICT:
PASS — DAY 7 COMPLETE
========================================================================================
```

The GraphX road network foundation is verified, structurally audited, thoroughly tested against production validation logic, and mathematically proven connected. Urbanix Day 7 is formally closed and ready for Day 8 dynamic shortest-path routing.
