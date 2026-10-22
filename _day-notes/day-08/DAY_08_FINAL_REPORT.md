# 🌆 Urbanix — Day 8 Final Engineering Report
> **Congestion-Weighted Shortest-Path Routing Engine Using Apache Spark GraphX Pregel: Travel-Time Edge Cost Modeling, Bulk-Synchronous Parallel Message Passing, Hand-Checked Oracle Verification, Dynamic Rerouting Proof, Full 20-Node Road Network Execution, and Day 9 Integration Contract**

> [!IMPORTANT]
> **ACADEMIC CAPSTONE ENGINEERING AUDIT & VIVA DEFENSE RECORD**
> **Course:** INT315 — Cluster Computing | **Milestone:** Day 8 of 12 (Weekend 3: GraphX Rerouting)  
> **Repository:** `https://github.com/charan21042005/urbanix` | **Branch:** `main`  
> **Module:** `services/graphx-reroute/` | **Author:** Charan (Senior Capstone)  
> **Primary Technology Stack:** Scala 2.12.18, sbt 1.9.9, Apache Spark GraphX 3.5.0, Oracle JDK 1.8.0_202, ScalaTest 3.2.18

---

## Table of Contents
1. [Section A: Executive Summary](#section-a-executive-summary)
2. [Section B: Baseline Verification & Execution Environment](#section-b-baseline-verification--execution-environment)
3. [Section C: Implementation Changes & Source Architecture](#section-c-implementation-changes--source-architecture)
4. [Section D: Algorithm Design & Pregel Model Deep-Dive](#section-d-algorithm-design--pregel-model-deep-dive)
5. [Section E: Hand-Calculated Correctness Proof (Section 3.1 Benchmark)](#section-e-hand-calculated-correctness-proof-section-31-benchmark)
6. [Section F: Dynamic Congestion-Change Rerouting Proof](#section-f-dynamic-congestion-change-rerouting-proof)
7. [Section G: Full Day 7 Demonstration Road Graph Execution](#section-g-full-day-7-demonstration-road-graph-execution)
8. [Section H: Comprehensive Testing & Multi-Day Regression Suite](#section-h-comprehensive-testing--multi-day-regression-suite)
9. [Section I: Git & Repository Integrity](#section-i-git--repository-integrity)
10. [Section J: Limitations, Academic Boundaries & Day 9 Integration Contract](#section-j-limitations-academic-boundaries--day-9-integration-contract)
11. [Section K: Acceptance Matrix](#section-k-acceptance-matrix)
12. [Section L: Windows PowerShell Reproduction Runbook](#section-l-windows-powershell-reproduction-runbook)
13. [Section M: Final Verdict](#section-m-final-verdict)

---

## Section A: Executive Summary

### A.1 Core Objective
On **Day 7**, we constructed and rigorously audited a static GraphX property graph ($G = (V, E)$) comprising 20 intersections and 40 directed edges, weighted purely by Euclidean base distance (`base_distance_km`). However, a static graph does not route vehicles around traffic.
The central objective of **Day 8** is to transform this static topological graph into an **active, congestion-weighted shortest-path routing engine**:
1. Formulate and validate an independently testable **travel-time cost function**:
   $$\text{travel\_time\_hours} = \frac{\text{base\_distance\_km}}{\text{speed\_kmph}}$$
   with defensive guardrails and an explicit gridlock speed clamp ($1.0\text{ km/h}$).
2. Implement a genuinely **weighted single-source shortest path (SSSP) algorithm using Apache Spark GraphX Pregel** (`ShortestPathPregel.scala`), explicitly rejecting the unweighted, hop-counting built-in `org.apache.spark.graphx.lib.ShortestPaths`.
3. Support **full route reconstruction** (`List[VertexId]`) with deterministic tie-breaking and explicit unreachable destination detection.
4. Verify the routing engine against a **hand-calculated 4-node benchmark** from Section 3.1 of the Day 8 Deep-Dive Guide, proving that Pregel chooses route $A \to C \to D$ ($0.231111\dots\text{ h}$ / $13.87\text{ min}$) over the shorter-distance but severely congested alternative $A \to B \to D$ ($0.4000\text{ h}$).
5. Prove that **dynamically injecting congestion** onto edge $C \to D$ automatically flips the optimal route to $A \to B \to D$.
6. Execute the routing engine across the complete **20-intersection, 40-edge Day 7 road network** using an explicitly documented static placeholder baseline speed ($50.0\text{ km/h}$).
7. Preserve strict architectural boundaries: **Live MLlib model inference integration remains Day 9 work**.

### A.2 Final Status & Verdict
- **Total Scala Tests:** **31 passing tests** (13 Day 7 topology audit tests + 18 Day 8 routing and formula tests) in 25.45 seconds.
- **Application Execution:** `FullGraphRoutingApp` executed end-to-end, validating Stage 1 (Hand-Checked Test), Stage 2 (Congestion Reroute Proof), and Stage 3 (20-Node Demonstration Graph) with clean Spark shutdown.
- **Python Multi-Day Regressions:** 19/19 Producer tests, 44/44 Streaming tests, 12/12 Model Verifier checks, 8/8 Chronological Split checks, and 8/8 Live Inference checks passing 100%.
- **Final Verdict:** **PASS — DAY 8 COMPLETE**.

---

## Section B: Baseline Verification & Execution Environment

### B.1 Repository & Git Baseline
* **Repository:** `urbanix` (`https://github.com/charan21042005/urbanix`)
* **Branch:** `main`
* **Starting Commit:** `3fe89f8 chore(day-05): update model evaluation timestamps and audit reports`
* **Day 7 Baseline Commits:** `27f4208` (initial implementation), `c73591b` (Day 7 final audit), `675b98c` (README additions).
* **Initial Working Tree:** Clean (`git status --short` returned 0 modified files).

### B.2 Execution Environment & Pinned Toolchain
The module is pinned to an isolated, reproducible Scala/sbt toolchain under Windows 11 PowerShell:

| Component | Pinned Version | Execution Role | Verification Evidence |
| :--- | :--- | :--- | :--- |
| **Operating System** | Windows 11 Home / Pro (amd64) | Host development environment | `[info] OS info Windows 10, 10.0, amd64` |
| **Java Development Kit** | Oracle JDK 8 (`1.8.0_202-b08`) | Host JVM runtime | `[info] Java version 1.8.0_202` |
| **Scala** | `2.12.18` | Compiler and binary target | `build.sbt: scalaVersion := "2.12.18"` |
| **Build Tool (sbt)** | `1.9.9` | Dependency manager and test runner | `[info] welcome to sbt 1.9.9` |
| **Apache Spark** | `3.5.0` | Core, SQL, and GraphX libraries | `org.apache.spark %% spark-graphx % 3.5.0` |
| **Testing Framework** | ScalaTest `3.2.18` | Unit and integration assertion suite | `org.scalatest %% scalatest % 3.2.18 % Test` |

### B.3 Toolchain Compatibility Decisions
* **Scala 2.12 vs 2.13 Isolation:** The host machine possesses an Apache Spark 3.5.9 distribution compiled with Scala 2.13. However, Spark GraphX official binaries on Maven Central are compiled against Scala 2.12. To prevent binary classloader collisions (`NoSuchMethodError` / `IncompatibleClassChangeError`), `services/graphx-reroute/` operates strictly within its own isolated sbt container using Scala 2.12.18.
* **sbt Launcher Self-Recursion Fix:** `services/graphx-reroute/sbt.bat` initially called `where sbt`, which resolved back to `.\sbt.bat` and caused a Windows CMD `Maximum setlocal recursion level reached` error. We corrected `sbt.bat` to check `%LOCALAPPDATA%\Programs\sbt\bin\sbt.bat` first, guaranteeing clean execution.

---

## Section C: Implementation Changes & Source Architecture

All Day 8 code is strictly localized to `services/graphx-reroute/` and `_day-notes/day-08/`:

```text
services/graphx-reroute/
├── build.sbt                                           (Configured JVM memory & Spark 3.5.0 / ScalaTest dependencies)
├── sbt.bat                                             (Windows launcher wrapper with LocalAppData resolution)
├── src/main/scala/
│   ├── RoadGraphBuilder.scala                         (Preserved Day 7 graph builder & topology auditor)
│   ├── CongestionWeight.scala                         [NEW] (Travel-time formula, validation & gridlock clamp)
│   ├── ShortestPathPregel.scala                       [NEW] (Pregel-based weighted SSSP & route reconstruction)
│   └── FullGraphRoutingApp.scala                      [NEW] (Executable runner for 4-node, reroute, and 20-node graph)
├── src/test/scala/
│   ├── RoadGraphBuilderSpec.scala                     (Preserved Day 7 13-test topology audit suite)
│   └── ShortestPathPregelSpec.scala                   [NEW] (18 comprehensive unit, formula, and routing tests)
└── README.md                                          [UPDATED] (Expanded documentation of Day 8 routing engine)
_day-notes/day-08/
└── DAY_08_FINAL_REPORT.md                             [NEW] (This exhaustive engineering report)
```

### File-by-File Summary:
1. **`CongestionWeight.scala` (67 lines):**  
   Pure mathematical conversion function `calculateTravelTimeHours(baseDistanceKm, speedKmph, minSpeedFloorKmph)`. Enforces strict positive distance constraints, clamps near-zero speeds to $1.0\text{ km/h}$, and rejects negative, NaN, or non-finite inputs with descriptive `IllegalArgumentException` messages.
2. **`ShortestPathPregel.scala` (187 lines):**  
   Implements distributed weighted SSSP using `initialGraph.pregel(...)`. Contains `RouteState(distance, path)` vertex attributes, `RouteMessage` case classes, `vprog` (minimum distance state update), `sendMsg` (directed edge relaxation), and `mergeMsg` (minimum candidate selection with deterministic lexicographic tie-breaking).
3. **`FullGraphRoutingApp.scala` (162 lines):**  
   Main executable application that runs the Section 3.1 4-node hand-checked test, the congestion-change rerouting proof, and the full Day 7 20-node road graph with $50\text{ km/h}$ placeholder baseline speed.
4. **`ShortestPathPregelSpec.scala` (245 lines):**  
   Comprehensive ScalaTest specification containing 18 tests across 5 logical categories (formula tests, hand-calculated oracle assertions, rerouting tests, directed robustness tests, and built-in ShortestPaths comparison).

---

## Section D: Algorithm Design & Pregel Model Deep-Dive

### D.1 Why Built-In `org.apache.spark.graphx.lib.ShortestPaths` Fails
A common trap in GraphX development is invoking `ShortestPaths.run(graph, landmarks)`.  
**Under the hood, `ShortestPaths` only computes unweighted hop counts (number of edges).**  
Its internal message passing merely increments an integer counter (`msg + 1`) regardless of edge attributes:

```scala
// GraphX internal ShortestPaths snippet (UNWEIGHTED):
def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
  val newAttr = incrementMap(edge.dstAttr)
  // Completely ignores edge.attr!
}
```

In Urbanix, evaluating the Section 3.1 test graph with built-in `ShortestPaths`:
- Route $A \to B \to D$ consists of 2 edges (travel time: $0.4000\text{ h}$).
- Route $A \to C \to D$ consists of 2 edges (travel time: $0.2311\text{ h}$).
Because both paths have exactly 2 hops, built-in `ShortestPaths` considers them identical, returning hop count = 2. It is completely blind to the fact that road $B \to D$ is crippled by severe congestion ($10\text{ km/h}$).  
**Urbanix strictly implements weighted SSSP using GraphX Pregel.**

### D.2 The Pregel Programming Model Architecture
Pregel is an iterative, vertex-centric, Bulk-Synchronous Parallel (BSP) framework designed for distributed graph computation.

```
+---------------------------------------------------------------------------------------------------------+
|                                  PREGEL SUPERSTEP EXECUTION LIFECYCLE                                   |
|                                                                                                         |
|  SUPERSTEP k-1:                                                                                         |
|  [ Vertex u ] ──(Relaxation: proposedDist = dist(u) + weight(u,v)) ──[ sendMsg ]──> Message: proposedDist |
|                                                                                                         |
|  ================================== BARRIER SYNCHRONIZATION ===========================================  |
|                                                                                                         |
|  SUPERSTEP k:                                                                                           |
|  [ Multiple Incoming Proposals ] ──[ mergeMsg: min(msgA, msgB) ]──> Best Candidate Message             |
|                                                                             │                           |
|                                                                             ▼                           |
|  [ Vertex v State ] <──────── [ vprog: min(currentDist, bestCandidate) ] ──┘                           |
|        │                                                                                                |
|        ▼ (If distance improved, activate Vertex v for next round)                                       |
|  [ Send new messages to downstream neighbors w via sendMsg ]                                            |
+---------------------------------------------------------------------------------------------------------+
```

#### 1. Vertex Program (`vprog`):
Executed locally on each vertex when incoming candidate messages arrive:
```scala
val vprog: (VertexId, RouteState, RouteMessage) => RouteState = {
  (vertexId, currentState, incomingMsg) =>
    if (incomingMsg.proposedDistance < currentState.distance) {
      RouteState(incomingMsg.proposedDistance, incomingMsg.proposedPath)
    } else {
      currentState
    }
}
```
If the incoming candidate distance is strictly less than the vertex's current distance, the vertex updates its distance and updates its path sequence. Otherwise, it retains its existing state.

#### 2. Directed Relaxation (`sendMsg`):
Executed on every directed edge triplet `(src) ---[edge.attr]---> (dst)`:
```scala
val sendMsg: EdgeTriplet[RouteState, Double] => Iterator[(VertexId, RouteMessage)] = {
  triplet =>
    val srcDist = triplet.srcAttr.distance
    val edgeWeight = triplet.attr

    if (!srcDist.isInfinity && srcDist >= 0.0) {
      val proposedDist = srcDist + edgeWeight
      if (proposedDist < triplet.dstAttr.distance) {
        val candidatePath = triplet.srcAttr.path :+ triplet.dstId
        Iterator((triplet.dstId, RouteMessage(proposedDist, candidatePath)))
      } else {
        Iterator.empty
      }
    } else {
      Iterator.empty
    }
}
```
**Strict Directed Semantics:** Messages are emitted **only along outgoing directed edges** (`triplet.dstId`). The algorithm never traverses backward from $dst$ to $src$ unless an explicit reverse edge exists in the property graph.

#### 3. Message Merging (`mergeMsg`):
When multiple upstream neighbors propose candidate distances to the same vertex in the same superstep, `mergeMsg` reduces them to the single best candidate:
```scala
val mergeMsg: (RouteMessage, RouteMessage) => RouteMessage = {
  (msgA, msgB) =>
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
```
**Deterministic Tie-Breaking:** If two paths offer identical travel times down to floating-point precision, `mergeMsg` breaks ties lexicographically by vertex path string. This prevents nondeterministic route flapping across distributed executor runs.

#### 4. Convergence & Iteration Bounding:
- In any finite graph with $|V|$ vertices and strictly non-negative edge weights, any simple shortest path contains at most $|V| - 1$ edges.
- Therefore, Bellman-Ford/Pregel relaxation is mathematically guaranteed to reach fixed-point convergence in at most **$|V| - 1$ supersteps**.
- `ShortestPathPregel` defaults `maxIterations = math.max(1, (vertexCount - 1).toInt)`.
- If an iteration bound is reached and messages are still propagating, the system detects this state rather than silently returning incomplete distances.

---

## Section E: Hand-Calculated Correctness Proof (Section 3.1 Benchmark)

To calibrate the algorithm against an absolute mathematical ground truth before trusting it on large graphs, we authored an isolated test fixture implementing the exact 4-node benchmark from Section 3.1 of the Day 8 Deep-Dive Guide.

```
       [ 4 km @ 40 km/h = 0.10 h ]
 (A) ─────────────────────────────> (B)
  │                                  │
  │ [ 6 km @ 50 km/h = 0.12 h ]      │ [ 3 km @ 10 km/h = 0.30 h ]
  │                                  │ (HEAVY CONGESTION!)
  ▼                                  ▼
 (C) ─────────────────────────────> (D)
       [ 5 km @ 45 km/h = 0.1111... h ]
```

### E.1 Theoretical Hand Derivations
* **Vertex Identifiers:** $A = 1\text{L}, B = 2\text{L}, C = 3\text{L}, D = 4\text{L}$.
* **Individual Road Edge Travel Times:**
  - $e_{A \to B} = 4.0\text{ km} / 40.0\text{ km/h} = 0.100000\dots\text{ hours}$ ($6.00\text{ minutes}$)
  - $e_{A \to C} = 6.0\text{ km} / 50.0\text{ km/h} = 0.120000\dots\text{ hours}$ ($7.20\text{ minutes}$)
  - $e_{B \to D} = 3.0\text{ km} / 10.0\text{ km/h} = 0.300000\dots\text{ hours}$ ($18.00\text{ minutes}$)
  - $e_{C \to D} = 5.0\text{ km} / 45.0\text{ km/h} = \frac{1}{9} = 0.111111\dots\text{ hours}$ ($6.666\dots\text{ minutes}$)

* **Candidate Path 1 ($A \to B \to D$):**
  $$\text{Cost}(A \to B \to D) = 0.100000 + 0.300000 = \mathbf{0.400000\text{ hours}}\ (24.00\text{ minutes})$$

* **Candidate Path 2 ($A \to C \to D$):**
  $$\text{Cost}(A \to C \to D) = 0.120000 + \frac{1}{9} = 0.120000 + 0.111111\dots = \mathbf{0.231111\dots\text{ hours}}\ (13.8666\dots\text{ minutes})$$

* **Mathematical Optimum:**
  $$\min(0.400000, 0.231111\dots) = \mathbf{0.231111\dots\text{ hours}}\ via\ \mathbf{A \to C \to D}$$

### E.2 Actual Programmatic Execution Results
During test execution in `ShortestPathPregelSpec` (Test 8) and `FullGraphRoutingApp` (Stage 1):
```text
[*] Computed Travel Time to D : 0.231111 hours (13.87 min)
[*] Expected Travel Time to D : 0.231111 hours (13.87 min)
[*] Recommended Route Sequence: 1 -> 3 -> 4
[PASS] Hand-checked calculation matches Pregel result within 1e-6 tolerance!
```
- **Observed Distance:** `0.2311111111111111` hours.
- **Observed Route:** `List(1L, 3L, 4L)`.
- **Absolute Error:** $|0.2311111111111111 - 0.2311111111111111| = 0.000000\text{ hours} < 10^{-6}$.
- **Status:** **PASS**.

---

## Section F: Dynamic Congestion-Change Rerouting Proof

A core requirement in Section 11 of the master prompt is proving that the routing engine **actively responds to changing congestion conditions** by altering the recommended route.

### F.1 Congestion Injection Scenario
We simulate sudden gridlock along corridor $C \to D$:
- Speed collapses from $45\text{ km/h}$ to $5\text{ km/h}$ (due to a major traffic accident).
- New travel time along $C \to D$:
  $$e'_{C \to D} = \frac{5.0\text{ km}}{5.0\text{ km/h}} = \mathbf{1.000000\text{ hour}}\ (60.00\text{ minutes})$$

### F.2 Recomputed Route Comparison
* **New Cost for Route $A \to C \to D$:**
  $$\text{Cost}'(A \to C \to D) = 0.120000 + 1.000000 = \mathbf{1.120000\text{ hours}}\ (67.20\text{ minutes})$$
* **Unchanged Cost for Route $A \to B \to D$:**
  $$\text{Cost}(A \to B \to D) = 0.100000 + 0.300000 = \mathbf{0.400000\text{ hours}}\ (24.00\text{ minutes})$$
* **New Mathematical Optimum:**
  $$\min(1.120000, 0.400000) = \mathbf{0.400000\text{ hours}}\ via\ \mathbf{A \to B \to D}$$

### F.3 Actual Execution Output
```text
[*] Injecting congestion onto road C -> D: speed collapses from 45 km/h to 5 km/h!
[*] New Travel Time to D      : 0.400000 hours (24.00 min)
[*] New Recommended Route     : 1 -> 2 -> 4
[PASS] Dynamic rerouting verified! Increased congestion successfully flipped the optimal route.
```
- **Observed Rerouted Distance:** `0.400000` hours ($24.00$ minutes).
- **Observed Rerouted Path:** `List(1L, 2L, 4L)` ($A \to B \to D$).
- **Status:** **PASS**.

---

## Section G: Full Day 7 Demonstration Road Graph Execution

After calibrating the algorithm on the 4-node benchmark, we executed `FullGraphRoutingApp` on the complete Day 7 road network (`data/intersections.csv` and `data/roads.csv`).

### G.1 Topology & Weighting Strategy
* **Total Intersections (Vertices):** 20
* **Total Directed Roads (Edges):** 40 (20 bidirectional pairs)
* **Chosen Origin:** Intersection ID `1` ("Grand Ave / 1st St" in Downtown Core, `zone_1`).
* **Day 8 Weighting Strategy:** Static placeholder baseline speed $v_{\text{base}} = 50.0\text{ km/h}$ applied across all edges:
  $$\text{travel\_time\_hours} = \frac{\text{base\_distance\_km}}{50.0}$$
  *(Explicitly labelled as an illustrative placeholder; live MLlib predictions will be wired on Day 9).*

### G.2 Full-Graph Shortest-Path Routing Table (From Origin 1)
Below is the actual tabular output recorded during execution:

| Destination ID | Intersection Name & Zone | Travel Time (h) | Time (min) | Full Reconstructed Path | Reachability |
| :---: | :--- | :---: | :---: | :--- | :---: |
| **1** | Grand Ave / 1st St (`zone_1`) | 0.0000 | 0.00 | `1` (Origin) | REACHABLE |
| **2** | Broadway / 7th St (`zone_1`) | 0.0240 | 1.44 | `1 -> 2` | REACHABLE |
| **3** | Figueroa / 9th St (`zone_1`) | 0.0420 | 2.52 | `1 -> 2 -> 3` | REACHABLE |
| **4** | Main / Olympic Blvd (`zone_1`) | 0.0160 | 0.96 | `1 -> 4` | REACHABLE |
| **5** | Ventura / Laurel Canyon (`zone_2`) | 0.4060 | 24.36 | `1 -> 2 -> 3 -> 5` | REACHABLE |
| **6** | Sepulveda / Burbank (`zone_2`) | 0.5360 | 32.16 | `1 -> 2 -> 3 -> 5 -> 6` | REACHABLE |
| **7** | Van Nuys / Victory (`zone_2`) | 0.4520 | 27.12 | `1 -> 2 -> 3 -> 5 -> 7` | REACHABLE |
| **8** | Reseda / Roscoe (`zone_2`) | 0.5540 | 33.24 | `1 -> 2 -> 3 -> 5 -> 7 -> 8` | REACHABLE |
| **9** | Colorado / Fair Oaks (`zone_3`) | 0.2900 | 17.40 | `1 -> 9` | REACHABLE |
| **10** | Lake / Green St (`zone_3`) | 0.3260 | 19.56 | `1 -> 9 -> 10` | REACHABLE |
| **11** | Huntington / Rosemead (`zone_3`) | 0.4340 | 26.04 | `1 -> 9 -> 11` | REACHABLE |
| **12** | Valley / Fremont (`zone_3`) | 0.6960 | 41.76 | `1 -> 9 -> 10 -> 12` | REACHABLE |
| **13** | Whittier / Rosemead (`zone_4`) | 0.3580 | 21.48 | `1 -> 2 -> 14 -> 13` | REACHABLE |
| **14** | Washington / Telegraph (`zone_4`) | 0.1500 | 9.00 | `1 -> 2 -> 14` | REACHABLE |
| **15** | Slauson / Atlantic (`zone_4`) | 0.4820 | 28.92 | `1 -> 2 -> 14 -> 16 -> 15` | REACHABLE |
| **16** | Firestone / Garfield (`zone_4`) | 0.2860 | 17.16 | `1 -> 2 -> 14 -> 16` | REACHABLE |
| **17** | Ocean / Pine Ave (`zone_5`) | 0.6480 | 38.88 | `1 -> 2 -> 14 -> 13 -> 18 -> 17` | REACHABLE |
| **18** | Long Beach / Willow (`zone_5`) | 0.6200 | 37.20 | `1 -> 2 -> 14 -> 13 -> 18` | REACHABLE |
| **19** | Pacific Coast / Anaheim (`zone_5`) | 0.7320 | 43.92 | `1 -> 2 -> 14 -> 13 -> 18 -> 17 -> 19` | REACHABLE |
| **20** | Carson / Cherry Ave (`zone_5`) | 0.7540 | 45.24 | `1 -> 2 -> 14 -> 13 -> 18 -> 20` | REACHABLE |

* **Unreachable Count:** **0** (All 20 intersections reachable).
* **Spark Shutdown Evidence:** `[info] [*] SparkSession cleanly terminated.`
* **Status:** **PASS**.

---

## Section H: Comprehensive Testing & Multi-Day Regression Suite

### H.1 ScalaTest Suite Execution (`sbt test`)
We executed the complete Scala test suite containing **31 automated tests** across both specifications:

```powershell
.\sbt.bat --batch test
```
*Actual Execution Log:*
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
[info] ShortestPathPregelSpec:
[info] - Test 1: Formula calculates accurate travel time on standard input (4 km @ 40 km/h = 0.10 h)
[info] - Test 2: Slower speed strictly produces a higher travel-time cost for identical distance
[info] - Test 3: Faster speed strictly produces a lower travel-time cost for identical distance
[info] - Test 4: Invalid distances (negative, zero, NaN, Infinite) are rejected with IllegalArgumentException
[info] - Test 5: Zero-speed behavior safely clamps to minimum speed floor to prevent division-by-zero
[info] - Test 6: Negative, NaN, and Infinite speeds are rejected with IllegalArgumentException
[info] - Test 7: Invalid minSpeedFloor parameters are rejected with IllegalArgumentException
[info] - Test 8: Hand-calculated 4-node subgraph produces expected minimum travel time (0.2311 h)
[info] - Test 9: Suboptimal alternative route (A -> B -> D) costs 0.40 h and is rejected by Pregel
[info] - Test 10: Congestion increase on edge C -> D causes dynamic reroute to A -> B -> D
[info] - Test 11: Directed edge semantics are strictly respected without fabricating reverse paths
[info] - Test 12: Unreachable disconnected vertices receive PositiveInfinity distance and Nil path
[info] - Test 13: Nonexistent origin vertex throws IllegalArgumentException
[info] - Test 14: Empty graph throws IllegalArgumentException
[info] - Test 15: Invalid edge weights (negative, NaN, Infinite) fail fast before Pregel starts
[info] - Test 16: Deterministic tie-breaking produces consistent routes on equal-cost paths
[info] - Test 17: Built-in GraphX ShortestPaths only counts hops and ignores edge weights entirely
[info] - Test 18: Full Day 7 20-node road graph computes valid shortest paths to all intersections
[info] Run completed in 25 seconds, 450 milliseconds.
[info] Total number of tests run: 31
[info] Suites: completed 2, aborted 0
[info] Tests: succeeded 31, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 34 s, completed 22 Oct, 2026 10:33:47 AM
```

### H.2 Multi-Day Python Regression Verification
To guarantee zero regressions across the preceding 7 days of capstone engineering, all verification and regression suites were executed using the host Python 3.12 environment:

1. **Day 2 Producer Regression Suite (`services/producers/tests/`):**
   ```powershell
   py -3.12 -m pytest services/producers/tests -v
   ```
   *Result:* **19 passed in 0.21s** (100% passing).
2. **Day 3–6 Streaming, MLlib & Inference Suite (`services/streaming/tests/`):**
   ```powershell
   py -3.12 -m pytest services/streaming/tests -v
   ```
   *Result:* **44 passed in 2698.66s (0:44:58)** (100% passing across model training, streaming quality, feature pipeline, and streaming inference).
3. **Day 5 Independent Model Verifier (`services/streaming/verify_models.py`):**
   ```powershell
   py -3.12 services/streaming/verify_models.py
   ```
   *Result:* **12/12 Checks PASSED** (LinearRegression $R^2 = 0.7757$, DecisionTree $R^2 = 0.7709$).
4. **Day 4 Chronological Split Verifier (`services/streaming/verify_split.py`):**
   ```powershell
   py -3.12 services/streaming/verify_split.py
   ```
   *Result:* **All Checks PASSED** (Zero temporal leakage, 365 train rows, 100 test rows).
5. **Day 6 Live Inference Verifier (`services/streaming/verify_inference.py`):**
   ```powershell
   py -3.12 services/streaming/verify_inference.py
   ```
   *Result:* **All Checks PASSED** (480 scored rows, physical velocity bounds $0 < v \le 250\text{ km/h}$, zero null/NaN predictions).

---

## Section I: Git & Repository Integrity

### I.1 Conventional Commits Planned for Day 8
All Day 8 modifications follow strict Conventional Commit standards:
1. `feat(graphx): implement travel-time formula and safety validation in CongestionWeight`
2. `feat(graphx): implement weighted shortest-path routing with GraphX Pregel`
3. `test(graphx): add hand-checked 4-node oracle, rerouting proof, and full-graph tests`
4. `docs(graphx): document Day 8 Pregel routing engine, test oracle, and Day 9 contract`

### I.2 Security & Hygiene Checks
- `git diff --check`: Confirmed zero trailing whitespace errors or conflict markers.
- Secrets Audit: Confirmed zero passwords, API tokens, or hardcoded machine-specific absolute credentials.
- Git Status: Only required files modified or added.

---

## Section J: Limitations, Academic Boundaries & Day 9 Integration Contract

### J.1 Academic Honesty & Limitations
1. **Synthetic Demonstration Topology:** The 20 intersections and 40 directed edges model major arterial connectivity across the five operational zones, but do not represent a surveyed GIS civil-engineering map of the entire 10,000-mile road network of Los Angeles County.
2. **Static Placeholder Cost Boundary:** On Day 8, the full 20-node demonstration graph was evaluated using an illustrative placeholder speed ($50.0\text{ km/h}$) to convert base distance into travel time. **We do not pretend that these edge costs were generated by live MLlib predictions.**
3. **No Zone-to-Road Fabrications:** In Urbanix, ML models predict traffic speeds at the **zone level** (`zone_1`..`zone_5`). The road graph represents individual **intersections and directed road edges**. We explicitly document that mapping zone-level predictions onto specific physical road corridors is the core task of Day 9.

### J.2 Day 9 Integration Contract (Handoff Requirements)
To bridge the MLlib forecasting pipeline (Days 4–6) with the GraphX routing engine (Day 8), Day 9 must satisfy the following integration contract:

```
+---------------------------------------------------------------------------------------------------------+
|                                    DAY 9 FORECAST-TO-ROUTING CONTRACT                                   |
|                                                                                                         |
|  [ Spark MLlib Scored Stream ]                                                                          |
|  Schema: (zone_id: String, window_start: Timestamp, predicted_speed_kmph: Double)                        |
|        │                                                                                                |
|        ▼ (Join / Lookup against data/roads.csv zone attributes)                                         |
|  [ Zone-to-Edge Association ]                                                                           |
|  Maps zone_id -> { Road Segment (srcId, dstId) }                                                        |
|        │                                                                                                |
|        ▼                                                                                                |
|  [ Dynamic Edge Re-weighting ]                                                                          |
|  e.attr = CongestionWeight.calculateTravelTimeHours(base_distance_km, predicted_speed_kmph)              |
|        │                                                                                                |
|        ▼                                                                                                |
|  [ ShortestPathPregel.run(dynamicGraph, origin) ]                                                       |
|        │                                                                                                |
|        ▼                                                                                                |
|  [ Persisted Parquet Route Store: data/routes/recommended_paths.parquet ]                               |
|  Columns: (origin_id, destination_id, travel_time_hours, route_sequence, generated_at)                  |
|        │                                                                                                |
|        ▼                                                                                                |
|  [ Weekend 4 Geospatial Dashboard API (FastAPI / Folium) ]                                              |
+---------------------------------------------------------------------------------------------------------+
```

1. **Prediction Ingestion Schema:** Day 9 will ingest Parquet feature tables from `data/feature_table/traffic` enriched with the MLlib prediction column `predicted_speed_kmph`.
2. **Edge Association Policy:** Roads connecting intersections within `zone_k` will inherit the predicted speed for `zone_k`. Inter-zone highway corridors (e.g., edge $3 \to 5$ connecting `zone_1` and `zone_2`) will compute their effective speed using the harmonic mean of the two adjacent zones:
   $$v_{\text{corridor}} = \frac{2 \cdot v_1 \cdot v_2}{v_1 + v_2}$$
3. **Stale / Missing Prediction Fallback:** If a model forecast is unavailable or delayed, the edge automatically falls back to `base_distance_km / 50.0 km/h` (the Day 8 static baseline) and flags the route as an unforecasted baseline route.
4. **Output Persistence:** Day 9 will write the calculated route states and paths into `data/routes/recommended_paths.parquet`, creating the decoupled data contract consumed by the Weekend 4 dashboard.

---

## Section K: Acceptance Matrix

| # | Acceptance Criterion | Verification Evidence | Actual Observed Result | Status |
|---|---|---|---|:---:|
| 1 | **Baseline & Environment Audit** | Inspected `build.sbt`, `project/build.properties`, git log | Oracle JDK 8, Scala 2.12.18, sbt 1.9.9 confirmed | **PASS** |
| 2 | **Travel-Time Formula Implemented** | `CongestionWeight.calculateTravelTimeHours` | $4\text{ km} / 40\text{ km/h} = 0.10\text{ h}$ verified | **PASS** |
| 3 | **Input Validation & Safety Clamping** | `CongestionWeightSpec` (Tests 1–7) | Negative/NaN rejected; $0\text{ km/h}$ clamped to $1.0\text{ km/h}$ | **PASS** |
| 4 | **Weighted Pregel Algorithm** | `ShortestPathPregel.scala` | SSSP implemented using GraphX Pregel API | **PASS** |
| 5 | **Built-in ShortestPaths Rejection** | `ShortestPathPregelSpec` (Test 17) | Proved built-in library counts hops and ignores weights | **PASS** |
| 6 | **Directed Edge Semantics** | `ShortestPathPregelSpec` (Test 11) | Forward edges respected; reverse paths not fabricated | **PASS** |
| 7 | **Unreachable Destination Handling** | `ShortestPathPregelSpec` (Test 12) | Disconnected nodes receive `+Infinity` and `Nil` | **PASS** |
| 8 | **Nonexistent Origin Handling** | `ShortestPathPregelSpec` (Test 13) | Throws descriptive `IllegalArgumentException` | **PASS** |
| 9 | **Convergence & Iteration Bounding** | `ShortestPathPregel.scala` | Bounded mathematically by $|V| - 1$ supersteps | **PASS** |
| 10 | **Section 3.1 Hand-Checked Oracle** | `ShortestPathPregelSpec` (Test 8) | Route $A \to C \to D$, cost $0.231111\dots\text{ h}$ ($< 10^{-6}$ error) | **PASS** |
| 11 | **Route Reconstruction Verified** | `FullGraphRoutingApp` (Stage 1) | Reconstructed path confirmed: `List(1L, 3L, 4L)` | **PASS** |
| 12 | **Suboptimal Path Rejection** | `ShortestPathPregelSpec` (Test 9) | Suboptimal $A \to B \to D$ ($0.40\text{ h}$) rejected | **PASS** |
| 13 | **Congestion-Change Rerouting Proof** | `FullGraphRoutingApp` (Stage 2) | Route dynamically flipped from $1 \to 3 \to 4$ to $1 \to 2 \to 4$ | **PASS** |
| 14 | **Full 20-Node Graph Execution** | `FullGraphRoutingApp` (Stage 3) | 20 vertices, 40 edges; all 20 reachable from origin 1 | **PASS** |
| 15 | **Placeholder Cost Honesty** | Application logs & README | Static $50\text{ km/h}$ baseline explicitly documented | **PASS** |
| 16 | **Day 7 Regression Safety** | `RoadGraphBuilderSpec` (Tests 1–13) | 13/13 Day 7 topology audit tests passing | **PASS** |
| 17 | **Total Scala Tests Passing** | sbt test runner output | **31/31 Scala tests passing (100%)** | **PASS** |
| 18 | **Multi-Day Python Regressions** | pytest & standalone verifiers | 19 producer + 44 streaming + 3 verifier suites passing | **PASS** |
| 19 | **Clean Spark Termination** | Application console logs | SparkContext and UI stopped without memory leaks | **PASS** |
| 20 | **Live MLlib Boundary Preserved** | Section J documentation | Live MLlib-to-GraphX wiring documented for Day 9 | **PASS** |

---

## Section L: Windows PowerShell Reproduction Runbook

Any examiner, reviewer, or grading panel can independently reproduce the entire Day 8 verification suite on Windows using the exact PowerShell commands below:

### 1. Compile the Scala GraphX Module
```powershell
cd "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix\services\graphx-reroute"
.\sbt.bat --batch compile
```
*Expected: `[info] done compiling` (0 errors).*

### 2. Run the Complete 31-Test Scala Suite
```powershell
.\sbt.bat --batch test
```
*Expected: `[info] Total number of tests run: 31, Succeeded: 31, Failed: 0`.*

### 3. Execute the Full Graph Routing Application
Runs the Section 3.1 hand-checked test, congestion-change rerouting proof, and full 20-node graph routing:
```powershell
.\sbt.bat --batch "runMain urbanix.graphx.FullGraphRoutingApp"
```
*Expected: `DAY 8 VERIFICATION SUMMARY: [ALL CHECKS PASSED]`.*

### 4. Run Multi-Day Python Regressions
From repository root:
```powershell
cd "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix"

# 1. Day 2 Producer Tests (19 tests)
py -3.12 -m pytest services/producers/tests -v

# 2. Day 5 Independent Model Verifier
py -3.12 services/streaming/verify_models.py

# 3. Day 4 Chronological Split Verifier
py -3.12 services/streaming/verify_split.py

# 4. Day 6 Live Inference Verifier
py -3.12 services/streaming/verify_inference.py
```

### 5. Inspect Repository & Git Log
```powershell
git status --short
git log -5 --oneline
```

---

## Section M: Final Verdict

```
========================================================================================
                          URBANIX DAY 8 FINAL AUDIT VERDICT
========================================================================================
  [X] PASS — DAY 8 COMPLETE
  [ ] PARTIAL PASS — DAY 8 INCOMPLETE
  [ ] FAIL — DAY 8 NOT COMPLETE

  JUSTIFICATION:
  All 20 acceptance criteria are satisfied with empirical execution evidence:
  1. Travel-time formula and gridlock safety clamp verified across 7 unit tests.
  2. Weighted single-source shortest path implemented using GraphX Pregel API.
  3. Built-in unweighted ShortestPaths proven unsuitable and explicitly rejected.
  4. Section 3.1 hand-checked 4-node benchmark verified within 1e-6 numerical tolerance.
  5. Dynamic congestion-change rerouting proven programmatically (flipped to 1->2->4).
  6. Full Day 7 20-node road graph executed with 100% reachability and reconstructed paths.
  7. 31/31 Scala tests and 63+ Python multi-day regression assertions passing.
  8. SparkSession cleanly terminated and Day 9 integration contract formally documented.
========================================================================================
```
