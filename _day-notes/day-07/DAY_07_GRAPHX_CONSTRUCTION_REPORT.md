# 🌐 Urbanix — Day 7 Scala + sbt Setup, Spark GraphX Road-Graph Construction & Pedagogical Defense Compendium
> **From Relational Tabular Predictions to Distributed Graph-Parallel Computing: JVM Bytecode Execution, Property Graph Abstractions, Vertex-Cut Partitioning, Bulk-Synchronous Parallel Pregel Primitives, Two-Phase Source Data Validation, and Verified Metropolitan Road-Network Topologies on Apache Spark GraphX**

> [!WARNING]
> **DEVELOPMENT NOTEBOOK, PEDAGOGICAL COMPENDIUM & ACADEMIC VIVA DEFENSE MANUAL**
> This document is the definitive, atomic, and exhaustive source of truth for **Day 7** of the Urbanix Smart City Platform. It is written to serve two critical purposes:
> 1. **Complete Beginner-to-Advanced Graph-Parallel Distributed Systems Compendium:** Explaining every distributed graph computing concept from absolute first principles (Relational Join Bottlenecks vs. Resilient Distributed Property Graphs, Scala Static Type Safety vs. Dynamic Python Typing, JVM Memory Model, SBT Dependency Tree Resolution and Binary ABI Compatibility, Edge-Cut vs. Vertex-Cut Graph Partitioning, Vertex Routing Tables, In-Memory Triplet Materialization, Bulk-Synchronous Parallel [BSP] Pregel Message Passing, Directed Multigraph Adjacency Representations, Weakly vs. Strongly Connected Components, Symmetrical Reciprocity Theorems, and Two-Phase Pre-Construction Source Auditing) so that any student, engineer, or academic evaluator reading from top to bottom can understand, reproduce, and explain the system completely.
> 2. **Academic & Industrial Viva Defense Playbook:** Equipping you with exact architectural blueprints, mathematical proofs, real terminal execution logs, step-by-step code walkthroughs, interactive `spark-shell` scripts, repository file paths, and model answers to 40 challenging examiner questions at the atomic level.

---

## Table of Contents
1. [Milestone Objective, Scope Boundaries & Architectural Transition](#1-milestone-objective-scope-boundaries--architectural-transition)
2. [Complete Beginner-to-Advanced Conceptual Foundations](#2-complete-beginner-to-advanced-conceptual-foundations)
3. [Python to Scala Rosetta Stone (Zero-Assumptions Guide)](#3-python-to-scala-rosetta-stone-zero-assumptions-guide)
4. [Build System, JVM Toolchain & Binary Compatibility Architecture](#4-build-system-jvm-toolchain--binary-compatibility-architecture)
5. [Dataset Design, Geospatial Topology & Provenance](#5-dataset-design-geospatial-topology--provenance)
6. [Source Code Deep-Dive (`RoadGraphBuilder.scala`)](#6-source-code-deep-dive-roadgraphbuilderscala)
7. [Automated Test Suite Deep-Dive (`RoadGraphBuilderSpec.scala`)](#7-automated-test-suite-deep-dive-roadgraphbuilderspecscala)
8. [Interactive Spark-Shell Exploration Playbook](#8-interactive-spark-shell-exploration-playbook)
9. [Step-by-Step Terminal Execution Commands](#9-step-by-step-terminal-execution-commands)
10. [Empirical Execution Logs & Count Reconciliation](#10-empirical-execution-logs--count-reconciliation)
11. [Multi-Day Regression Matrix & Invariance Proof](#11-multi-day-regression-matrix--invariance-proof)
12. [Scope Boundaries & Day 8 Integration Interface](#12-scope-boundaries--day-8-integration-interface)
13. [Comprehensive Academic Viva Voce Defense Playbook (40 Questions & Answers)](#13-comprehensive-academic-viva-voce-defense-playbook-40-questions--answers)
14. [Final Acceptance Gate Matrix & Closure Decision](#14-final-acceptance-gate-matrix--closure-decision)

---

## 🎯 1. Milestone Objective, Scope Boundaries & Architectural Transition

### 1.1 The Context: From Ingestion and Forecasting to Active Rerouting
In **Days 1 through 6**, the Urbanix platform established the distributed ingestion infrastructure, telemetry replay layer, streaming aggregation engine, feature preparation pipeline, supervised model evaluation, and live streaming inference:
- **Day 1:** Local distributed infrastructure provisioned via Docker Compose: Zookeeper, Apache Kafka broker (topics: `traffic-topic`, `aqi-topic`, `weather-topic`), and Apache Spark Standalone cluster (Master + Worker).
- **Day 2:** Sensor simulation layer in Python, continuously replaying historical telemetry from Caltrans PeMS loop detectors (METR-LA), U.S. EPA air quality stations, and OpenWeather archives into Kafka with asynchronous delivery callbacks, retries, and process supervision (`run_all.py`).
- **Day 3:** Apache Spark Structured Streaming engine aggregating raw Kafka JSON streams into 5-minute tumbling windows governed by a 2-minute event-time watermark, enriched with static municipal zone metadata (`data/grid_zones.csv`), and persisted into independent Parquet feature tables under `data/feature_table/{traffic,aqi,weather}`.
- **Day 4:** Feature Engineering and Chronological Train/Test Split Pipeline: Built a 300-second Continuity Guard, assembled a 9-dimensional `VectorUDT` feature vector ($\mathbf{x} \in \mathbb{R}^9$), formulated the future target velocity ($y = \text{next\_avg\_speed\_kmph}$), enforced strict estimator hygiene (`StringIndexer` fit on training split only), established an 80% elapsed-time chronological cutoff (`2012-03-01 06:24:00 UTC`), excluded 5 boundary-crossing rows, and persisted 365 training rows (`data/train/`) and 100 held-out testing rows (`data/test/`) with zero temporal leakage.
- **Day 5:** Model Training, Evaluation, and Reproducibility: Trained and evaluated two native Spark MLlib estimators. Linear Regression achieved an aggregate $R^2 = 0.7757$, an RMSE of $2.9280\text{ km/h}$, and an MAE of $2.4733\text{ km/h}$, surpassing the project milestone ($R^2 > 0.75$). The fitted Linear Regression model was persisted natively under `models/linear_regression/`.
- **Day 6:** Live MLlib Streaming Inference and Durable Checkpoint Recovery: Deployed real-time model scoring inside a `foreachBatch` streaming sink, verified dynamic feature parity matching Day 4/5 vectors, enforced physical velocity bounds ($0 < v \le 250\text{ km/h}$), and demonstrated resilient checkpoint recovery after hard OS-level process termination (`taskkill /F`).

### 1.2 Entering Weekend 3: The Need for Graph-Parallel Architecture
With **Day 7**, Urbanix enters **Weekend 3: GraphX Rerouting Engine**. 
Predicting traffic congestion is only half the smart city mission. Once the platform forecasts that an arterial corridor (such as US-101 or I-110) will experience severe congestion in the next 15 minutes, the system must compute alternate, optimal detour paths across the city's road network.

Standard relational processing engines (like SQL or PySpark DataFrames) represent graphs as flat tabular structures:
```sql
SELECT * FROM roads WHERE src_id = 1 AND dst_id = 2;
```
To find a multi-hop path of length $k$ across a relational table requires $k-1$ successive relational self-joins:
$$\text{roads} \bowtie_{\text{dst\_id}=\text{src\_id}} \text{roads} \bowtie_{\text{dst\_id}=\text{src\_id}} \dots \bowtie \text{roads}$$
In a distributed cluster, each join forces a full network shuffle across all worker nodes, generating massive intermediate state explosion, disk spill, and network saturation.

**Apache Spark GraphX** completely eliminates this bottleneck by modeling graphs as **Resilient Distributed Property Graphs**:
1. Graphs are partitioned using **Vertex-Cut Partitioning**, colocating edge partitions with vertex routing tables.
2. Traversal and shortest-path calculations are executed using the **Pregel Bulk-Synchronous Parallel (BSP)** message-passing abstraction, where vertices compute and communicate iteratively in memory with zero relational join overhead.

Because **GraphX is exclusively a native JVM/Scala API** (with no official PySpark bindings), Day 7 transitions the Urbanix project from Python into a dedicated, hermetic **Scala 2.12 and sbt 1.9.9** module under `services/graphx-reroute/`.

```
+-------------------------------------------------------------------------------------------------------------------------+
|                                                URBANIX DAY 7 ARCHITECTURE                                               |
|                                                                                                                         |
|  [ data/intersections.csv ] (20 rows)                         [ data/roads.csv ] (40 rows)                              |
|            │                                                               │                                            |
|            ▼                                                               ▼                                            |
|  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │  PHASE 1: PRE-CONSTRUCTION SOURCE DATA VALIDATION (RoadGraphBuilder.validateSourceData)                           │  |
|  │  1. Node ID Integrity: 20 distinct IDs, 0 duplicate IDs, 0 null/invalid IDs                                       │  |
|  │  2. Name Integrity: 0 blank/null intersection names                                                               │  |
|  │  3. Road Record Integrity: 40 directed records, 0 exact duplicate (u,v) pairs                                      │  |
|  │  4. Road Geometry Integrity: 0 invalid endpoints (all resolve to 1..20), 0 non-positive/NaN/Inf distances        │  |
|  │  5. Self-Loop Check: 0 records with src_id == dst_id                                                               │  |
|  │  ==> Validation Status: [PASS] VALID (Strict fail-fast policy enforced in production)                             │  |
|  └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘  |
|            │                                                               │                                            |
|            ▼                                                               ▼                                            |
|  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │  PHASE 2: DISTRIBUTED GRAPHX PROPERTY GRAPH CONSTRUCTION (RoadGraphBuilder.buildGraph)                           │  |
|  │  - VertexRDD[(VertexId, String)]: 20 vertices [(1L, "Grand Ave & 1st St"), ..., (20L, "PCH & 7th St")]             │  |
|  │  - EdgeRDD[Edge[Double]]: 40 directed edges [Edge(1L, 2L, 1.2), Edge(2L, 1L, 1.2), ..., Edge(20L, 18L, 6.7)]     │  |
|  │  - Graph[String, Double] materialized in-memory across cluster partitions                                         │  |
|  └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘  |
|            │                                                                                                            |
|            ▼                                                                                                            |
|  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  |
|  │  PHASE 3: GRAPH INTEGRITY & TOPOLOGY AUDIT (RoadGraphBuilder.auditGraph)                                          │  |
|  │  - Vertex Count Reconciliation: Source (20) == Graph (20)                                                          │  |
|  │  - Edge Count Reconciliation: Source (40) == Graph (40)                                                            │  |
|  │  - Dangling Edge Detection: 0 edges point to nonexistent nodes                                                     │  |
|  │  - Phantom Vertex Detection: 0 unreferenced dummy vertices auto-created by GraphX                                  │  |
|  │  - Structural Self-Loop Detection: 0 self-loops                                                                   │  |
|  │  - Connected Components: Exactly 1 Weakly Connected Component (ConnectedComponents.run)                          │  |
|  │  - Directed Symmetry (Reciprocity): 100% Symmetrical (Every u->v has corresponding v->u)                          │  |
|  │  - Strong Connectivity Proof: 100% Reciprocity ==> Weak Connectivity == Strong Connectivity (SCC = 1)             │  |
|  └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘  |
|            │                                                                                                            |
|            ▼                                                                                                            |
|  [ Ready for Day 8 Congestion-Weighted Shortest Path Routing via Pregel / Dijkstra ]                                   |
|                                                                                                                         |
+-------------------------------------------------------------------------------------------------------------------------+
```

---

## 🔬 2. Complete Beginner-to-Advanced Conceptual Foundations

### 2.1 Graph Theory 101: Formal Mathematical Taxonomy
In computer science, discrete mathematics, and operations research, a network is formalized as a **Graph**:
$$\mathcal{G} = (V, E)$$
Where:
- **$V$ (Vertices / Nodes):** The finite, non-empty set of discrete entities. Formally:
  $$V = \{v_1, v_2, \dots, v_n\}, \quad |V| = n$$
  In Urbanix Day 7, each vertex represents a physical road intersection or freeway interchange ($|V| = 20$).
- **$E$ (Edges / Arcs / Links):** The set of pairwise relationships connecting vertices. Formally:
  $$E = \{e_1, e_2, \dots, e_m\}, \quad |E| = m$$
  Where each directed edge $e = (u, v)$ originates at source vertex $u \in V$ and terminates at destination vertex $v \in V$. In Urbanix Day 7, $|E| = 40$.

#### Graph Classification Taxonomy
1. **Undirected vs. Directed Graphs (Digraphs):**
   - In an *undirected graph*, edges are unordered sets $\{u, v\}$. Traversal is bidirectional by definition.
   - In a *directed graph*, edges are ordered pairs $(u, v)$. Traversal is permissible only in the specified direction $u \to v$.
2. **Simple Graphs vs. Multigraphs:**
   - A *simple graph* permits at most one edge between any ordered pair $(u, v)$ and contains zero self-loops.
   - A *multigraph* permits multiple parallel edges between the same vertex pair (e.g., parallel express and local freeway lanes). **GraphX is natively a Directed Multigraph engine.**
3. **Paths, Walks, Trails, and Cycles:**
   - **Walk:** An alternating sequence of vertices and edges $(v_0, e_1, v_1, e_2, \dots, v_k)$.
   - **Trail:** A walk where all edges are distinct.
   - **Path:** A walk where all vertices (and thus all edges) are strictly distinct.
   - **Cycle:** A closed path where $v_0 = v_k$ and all intermediate vertices are distinct.
4. **Self-Loops:**
   - An edge where the source and destination are identical: $e = (u, u)$. In physical road networks, self-loops represent non-physical dead-end loops or data corruption anomalies. Our production validator explicitly checks for and forbids self-loops.

#### Why Road Networks Must Be Directed
In elementary cartography, a street is often drawn as a simple line. However, in transportation systems engineering, modeling bidirectional roads as undirected segments is mathematically invalid:
$$e_{\text{forward}} = (u, v) \quad \text{and} \quad e_{\text{reverse}} = (v, u)$$
1. **Asymmetric Directional Congestion:** During morning rush hour, traffic moving from suburban residential zones (`zone_2`) toward the downtown commercial center (`zone_1`) crawls at $15\text{ km/h}$, while outbound traffic flows freely at $85\text{ km/h}$. Undirected edges cannot assign distinct directional speeds.
2. **One-Way Grids:** Modern urban layouts contain one-way arterials where vehicular travel is legally prohibited in the reverse direction.
3. **Independent Dynamic Weights:** Modeling each direction as a separate directed edge allows Day 8 to attach independent dynamic travel times:
   $$t(u \to v) \ne t(v \to u)$$

---

### 2.2 Classical Graph Data Structures vs. GraphX Distributed RDPG
Before examining distributed graph processing, we must understand how graphs are stored in memory and why classical single-machine data structures fail on distributed clusters.

#### 1. Adjacency Matrix ($A \in \{0, 1\}^{n \times n}$)
A two-dimensional square matrix where cell $A[u, v] = 1$ if edge $(u, v) \in E$, and $0$ otherwise (or stores edge weight $w$).
$$\begin{pmatrix} 0 & 1.2 & 0 & 0.8 \\ 1.2 & 0 & 0.9 & 0 \\ 0 & 0.9 & 0 & 0 \\ 0.8 & 0 & 0 & 0 \end{pmatrix}$$
- **Space Complexity:** $\Theta(|V|^2)$. For a metropolitan network with $100,000$ intersections, an adjacency matrix requires $100,000^2 \times 8\text{ bytes} \approx 80\text{ Gigabytes}$ of contiguous memory, even though $99.99\%$ of cells are zeroes!
- **Edge Lookup Time:** $\mathcal{O}(1)$.
- **Neighbor Iteration Time:** $\mathcal{O}(|V|)$ (must scan an entire row of zeroes).

#### 2. Adjacency List ($Adj[u] = [(v_1, w_1), (v_2, w_2), \dots]$)
An array of $|V|$ linked lists or dynamic arrays, where entry $Adj[u]$ contains all outgoing neighbors of vertex $u$.
- **Space Complexity:** $\Theta(|V| + |E|)$. Highly compact for sparse graphs.
- **Neighbor Iteration Time:** $\mathcal{O}(\text{deg}(u))$.
- **Distributed Failure:** In a distributed cluster, high-degree hub vertices have massive adjacency lists. If a single vertex has $100,000$ neighbors, its entire list must reside in one machine's RAM, causing severe executor out-of-memory (OOM) imbalances.

#### 3. Edge List / Coordinate Format ($[(u, v, w)]$)
An unordered array of edge tuples. This is the flat relational table representation used by standard SQL and CSV files.
- **Space Complexity:** $\Theta(|E|)$.
- **Traversal Failure:** Finding neighbors requires a full scan or expensive relational join ($\mathcal{O}(|E|)$). Multi-hop path finding requires $k-1$ successive distributed shuffles.

#### 4. GraphX Resilient Distributed Property Graph (Compressed Sparse Partitions)
GraphX combines the compactness of Compressed Sparse Row (CSR) / Column (CSC) formats with distributed vertex-cut partitioning. Inside each executor partition, edges are stored in continuous primitive arrays (`Array[VertexId]`, `Array[Int]`, `Array[ED]`), completely eliminating Java object pointer overhead.

#### Comprehensive Complexity Comparison Table
| Feature / Operation | Adjacency Matrix | Adjacency List | Relational Edge List (SQL) | GraphX RDPG (Distributed) |
| :--- | :---: | :---: | :---: | :---: |
| **Storage Complexity** | $\mathcal{O}(\|V\|^2)$ | $\mathcal{O}(\|V\| + \|E\|)$ | $\mathcal{O}(\|E\|)$ | $\mathcal{O}(\|V_{\text{rep}}\| + \|E\|)$ distributed |
| **Edge Existence $(u, v)$** | $\mathcal{O}(1)$ | $\mathcal{O}(\text{deg}(u))$ | $\mathcal{O}(\|E\|)$ (or index scan) | $\mathcal{O}(\log(\text{deg}(u)))$ local binary search |
| **Iterate Out-Neighbors** | $\mathcal{O}(\|V\|)$ | $\mathcal{O}(\text{deg}(u))$ | $\mathcal{O}(\|E\|)$ join | $\mathcal{O}(\text{deg}(u))$ local array slice |
| **Memory Overhead** | Extreme for sparse graphs | Pointer bloat on JVM | Relational tuple headers | **Zero pointer overhead (primitive arrays)** |
| **Cluster Partitioning** | Block cyclic matrix cut | Vertex-cut (unbalanced) | Hash partition on `src_id` | **Balanced Vertex-Cut with 2D routing** |
| **Multi-Hop Traversal** | Matrix multiplication | BFS / DFS recursion | Cartesian network shuffles | **Pregel supersteps (zero join shuffles)** |

---

### 2.3 Graph Topology Metrics, Physical Laws & Planarity

#### Degree Concepts & The Handshaking Lemma
For any vertex $v \in V$:
- **In-Degree ($d_{\text{in}}(v)$):** The number of incoming directed edges arriving at $v$:
  $$d_{\text{in}}(v) = |\{(u, v) \in E\}|$$
- **Out-Degree ($d_{\text{out}}(v)$):** The number of outgoing directed edges originating from $v$:
  $$d_{\text{out}}(v) = |\{(v, w) \in E\}|$$
- **Total Degree ($d(v)$):** $d(v) = d_{\text{in}}(v) + d_{\text{out}}(v)$.

> **The Directed Handshaking Lemma:**
> In any directed multigraph $\mathcal{G} = (V, E)$, the sum of all in-degrees equals the sum of all out-degrees, which equals the total number of edges:
> $$\sum_{v \in V} d_{\text{in}}(v) = \sum_{v \in V} d_{\text{out}}(v) = |E|$$

In Urbanix Day 7:
$$\sum_{v=1}^{20} d_{\text{in}}(v) = \sum_{v=1}^{20} d_{\text{out}}(v) = 40$$
Our automated tests verify this exact conservation law across the entire GraphX topology.

#### Planar Graphs & Road Networks
A graph is **Planar** if it can be drawn in the Euclidean 2D plane such that no two edges cross each other.
- **Euler's Formula for Planar Graphs:** For any connected planar graph with $V$ vertices, $E$ edges, and $F$ enclosed faces:
  $$|V| - |E| + |F| = 2$$
- **Edge Sparsity Corollary:** In any simple planar graph without triangles, $|E| \le 2|V| - 4$. In general planar graphs:
  $$|E| \le 3|V| - 6$$
- **Application to Smart City Road Networks:** Road networks are **Quasi-Planar Spatial Networks**. Unlike social networks (where a celebrity node can connect to $50,000,000$ followers, making $|E| \approx O(|V|^2)$), road intersections are physically constrained by geography: an intersection rarely connects to more than 4 to 6 physical road corridors ($d(v) \le 6$). Even with multi-level freeway overpasses (grade separations that cross without intersecting), the average degree remains bounded: $\bar{d} \approx 2.5 \text{ to } 4.0$. This sparsity makes road graphs exceptionally well-suited for linear-time traversal algorithms.

---

### 2.4 The GraphX Resilient Distributed Property Graph (RDPG) Model
Apache Spark GraphX formalizes the road network as a **Resilient Distributed Property Graph**:
$$\text{Graph}[VD, ED]$$
Where:
- `VD` is the **Vertex Attribute** type (e.g., `String` for intersection name, or a custom `case class IntersectionData(name: String, zoneId: String, lat: Double, lon: Double)`).
- `ED` is the **Edge Attribute** type (e.g., `Double` for `base_distance_km`, or `case class RoadData(lengthKm: Double, speedLimit: Double)`).

GraphX represents the property graph as two specialized, distributed RDDs:
1. `VertexRDD[(VertexId, VD)]`: An RDD of key-value pairs where `VertexId` is always a 64-bit signed Long integer (`Long`), and `VD` is the attached property.
2. `EdgeRDD[Edge[ED]]`: An RDD of `Edge` case classes containing `srcId: Long`, `dstId: Long`, and `attr: ED`.

```
VertexRDD[(VertexId, VD)]                       EdgeRDD[Edge[ED]]
+----+----------------------------+            +-------+-------+--------------------+
| ID | VD (Intersection Name)     |            | srcId | dstId | attr (Distance km) |
+----+----------------------------+            +-------+-------+--------------------+
| 1L | "Grand Ave & 1st St"       |            | 1L    | 2L    | 1.20               |
| 2L | "Broadway & 7th St"        |            | 2L    | 1L    | 1.20               |
| 3L | "Figueroa & Wilshire Blvd" |            | 1L    | 4L    | 0.80               |
+----+----------------------------+            +-------+-------+--------------------+
```

---

### 2.5 Low-Level Memory Layout & Primitive Unboxing on the JVM
Why does GraphX implement its own specialized RDDs (`VertexRDD` and `EdgeRDD`) rather than using standard Spark `RDD[(Long, VD)]` or `RDD[Edge[ED]]`?

#### The JVM Object Overhead Tax
In standard Java/Scala, an ordinary object in the 64-bit JVM incurs substantial memory overhead:
- **Object Header:** 12 to 16 bytes (Mark Word + Klass Word).
- **Reference Pointers:** 4 to 8 bytes (Compressed Oops vs 64-bit pointers).
- **Padding:** 8-byte boundary alignment.

If a road network with 10 million vertices stores vertex IDs as standard boxed `java.lang.Long` objects:
$$\text{Boxed Long} = 16\text{ bytes (header)} + 8\text{ bytes (payload)} = 24\text{ bytes}$$
Adding the reference pointer in an array adds another 8 bytes $\to \mathbf{32\text{ bytes}}$ per 8-byte integer! That represents a **$300\%$ memory bloat** purely due to JVM object overhead, leading to massive GC pauses during graph traversal.

#### The GraphX Solution: Specialized Primitive Arrays & Off-Heap CSR
GraphX eliminates object overhead through specialized columnar array partitioning:
1. **Primitive Arrays:** Vertex IDs and edge pointers are stored in flat, continuous `Array[Long]` and `Array[Int]` allocations.
2. **Compressed Sparse Row (CSR) Representation:** Inside each `EdgePartition`, edges originating from the same vertex are stored contiguously in memory. An array of integer offsets (`srcOffsets: Array[Int]`) marks where each vertex's adjacency slice begins and ends.
3. **Cache Line Locality:** Iterating through the outgoing edges of an intersection becomes a single contiguous memory scan across CPU L1/L2/L3 caches, achieving maximum hardware throughput.

---

### 2.6 Distributed Graph Partitioning: Vertex-Cut vs. Edge-Cut
How does an engine split a graph with millions of vertices and edges across a cluster of 50 worker machines?

#### Approach A: Edge-Cut Partitioning (Traditional HPC Graph Tools)
Vertices are partitioned evenly across machines. Any edge that connects a vertex on Machine 1 to a vertex on Machine 2 is "cut" across the network:
```
Machine 1                   Machine 2
[ Vertex A ] ═════ CUT ═════> [ Vertex B ]
[ Vertex C ]                  [ Vertex D ]
```
**Fatal Flaw in Real-World Networks:** Real-world networks exhibit a **Power-Law Degree Distribution** ($P(k) \sim k^{-\gamma}$). High-degree hub intersections (e.g., major freeway interchanges) connect to many edges. Under edge-cut partitioning, cutting the edges of a hub vertex forces continuous, high-volume network shuffles whenever that hub sends or receives messages.

#### Approach B: Vertex-Cut Partitioning (Apache Spark GraphX)
GraphX inverts this paradigm: **edges are partitioned evenly across machines, while vertices are cut (replicated) across partitions**:
```
Partition 1 (Machine 1)                   Partition 2 (Machine 2)
[ Vertex A ] ─── Edge 1 ───> [ Vertex B* ]   [ Vertex B ] ─── Edge 2 ───> [ Vertex C ]
(Primary)                    (Mirror)        (Primary)
```
- **Edge Colocation:** Each edge exists on exactly one partition.
- **Vertex Routing Tables:** Vertices that participate in edges across multiple partitions are replicated as "mirrors" on those partitions.
- **Why Vertex-Cut Wins:** In road networks, $|E| \approx 2 \times |V|$ to $4 \times |V|$. Distributing edges evenly ensures balanced storage and computation. When a vertex attribute updates, GraphX multicasts the update only to the machines hosting mirrors of that vertex, drastically reducing cluster network traffic.

#### GraphX Partitioning Strategies (`PartitionStrategy`)
GraphX provides multiple built-in vertex-cut strategies:
1. `RandomVertexCut`: Edges are assigned to partitions by computing a hash of the source and destination IDs: `hash(srcId, dstId) % numParts`. Fast, but produces higher vertex replication.
2. `CanonicalRandomVertexCut`: Assigns edges based on the canonical undirected pair: `hash(min(src, dst), max(src, dst)) % numParts`. Colocates forward edge $(u, v)$ and reverse edge $(v, u)$ onto the exact same partition. **Ideal for road networks!**
3. `EdgePartition2D`: Arranges partitions into a 2D grid matrix, guaranteeing that any vertex is replicated across at most $2\sqrt{\text{numParts}} - 1$ machines.

---

### 2.7 Vertex Routing Tables & Mirror Replication Factor
To manage replicated vertices without performing broadcast shuffles, GraphX constructs a **Routing Table** on each partition.
- **The Replication Factor:** The average number of physical cluster partitions on which a vertex is replicated:
  $$\rho = \frac{\sum_{v \in V} \text{mirrors}(v)}{|V|}$$
  In a well-partitioned road network, $\rho$ typically ranges between $1.1$ and $1.5$, meaning very few vertices require cross-machine replication.
- **Bitmap Tracking:** Each partition maintains a bitmask indicating whether a vertex appears as a source, a destination, or both within that partition's local edge slice.

---

### 2.8 The Edge Triplet Abstraction
To compute routes or evaluate edge validity, an algorithm frequently needs to examine the source vertex, the connecting edge, and the destination vertex simultaneously. In SQL, this requires two joins (`roads` JOIN `vertices` ON `src` JOIN `vertices` ON `dst`).

In GraphX, this is materialized through the **Edge Triplet**:
$$\text{EdgeTriplet}[VD, ED]$$

```
Edge Triplet:
+----------------------------------------------------------------------------------------+
|  Source Vertex: (srcId: 1L, srcAttr: "Grand Ave & 1st St")                             |
|  Edge Attribute: (attr: 1.2 km)                                                        |
|  Destination Vertex: (dstId: 2L, dstAttr: "Broadway & 7th St")                        |
+----------------------------------------------------------------------------------------+
```

Inside Spark, `graph.triplets` views the graph as an RDD of triplets:
```scala
graph.triplets.foreach { triplet =>
  println(s"${triplet.srcAttr} connects to ${triplet.dstAttr} across ${triplet.attr} km")
}
```
GraphX optimizes triplet materialization by colocating the vertex attributes with the edge partitions via the Vertex Routing Table, eliminating shuffles during triplet iteration.

---

### 2.9 The GraphX Operator Zoo: Four Functional Families
GraphX exposes an expressive, purely functional API organized into four primary operator families:

```
                          ┌───────────────────────────┐
                          │   GraphX Operator Zoo     │
                          └─────────────┬─────────────┘
          ┌─────────────────────┬───────┴─────────────┬─────────────────────┐
          ▼                     ▼                     ▼                     ▼
   1. Structural         2. Property           3. Join               4. Neighborhood
   - reverse             - mapVertices         - joinVertices        - aggregateMessages
   - subgraph            - mapEdges            - outerJoinVertices   - inDegrees / outDegrees
   - mask                - mapTriplets                               - collectNeighborIds
```

1. **Structural Operators:** Modify graph topology without altering vertex or edge attributes:
   - `reverse`: Flips the direction of every edge ($u \to v$ becomes $v \to u$).
   - `subgraph(epred, vpred)`: Filters the graph, retaining only vertices and edges satisfying user-defined predicate functions.
   - `mask(anotherGraph)`: Retains only the topological intersection of two graphs.
2. **Property Operators:** Transform attribute payloads while preserving topology:
   - `mapVertices((id, attr) => newAttr)`: Pure function updating vertex data.
   - `mapEdges(edge => newAttr)`: Pure function updating edge weights.
   - `mapTriplets(triplet => newAttr)`: Updates edge weights with access to source and destination vertex attributes.
3. **Join Operators:** Enrich graph attributes from external distributed RDDs:
   - `joinVertices(tableRDD)((id, oldAttr, tableVal) => newAttr)`: Inner join with external RDD.
   - `outerJoinVertices(tableRDD)((id, oldAttr, optTableVal) => newAttr)`: Left outer join handling missing keys via `Option[T]`.
4. **Neighborhood Aggregation (`aggregateMessages`):**
   The computational engine powering Pregel. Replaces the legacy `mapReduceTriplets`:
   ```scala
   graph.aggregateMessages[Double](
     sendMsg = triplet => {
       // Sends a message along outgoing edges
       triplet.sendToDst(triplet.srcAttr + triplet.attr)
     },
     mergeMsg = (a, b) => math.min(a, b) // Combines incoming messages at destination
   )
   ```

---

### 2.10 The Pregel Bulk-Synchronous Parallel (BSP) Computation Model
How will Day 8 compute the shortest path across the graph? GraphX implements Google's **Pregel** framework, which follows the **Bulk-Synchronous Parallel (BSP)** computational model:

```
Superstep 0:
All vertices initialized with distance: Origin = 0.0, All Others = Infinity.
Origin sends message along outgoing edges: "Distance to my neighbor = 0.0 + edge_weight".

--------------------------- SYNCHRONIZATION BARRIER ---------------------------

Superstep 1:
Neighbors receive messages, compute min(incoming_messages).
If new distance < current distance:
   Update current distance.
   Send updated distance + edge_weight along outgoing edges.
Else:
   Vote to halt (inactive).

--------------------------- SYNCHRONIZATION BARRIER ---------------------------

Superstep 2..N:
Repeat until all vertices vote to halt and no messages remain in transit.
```
This message-passing approach converges in $O(\text{diameter})$ supersteps without a single relational join.

---

### 2.11 Graph Connectivity Theory: Weakly vs. Strongly Connected Components
A fundamental question in smart city road networks is **reachability**: *Can an emergency vehicle travel from any intersection to every other intersection in the city?*

1. **Undirected Graph Connectivity:** An undirected graph is connected if there is a path between every pair of vertices.
2. **Weakly Connected Component (WCC):** A directed graph is *weakly connected* if replacing all directed edges with undirected edges produces a connected undirected graph.
   - GraphX provides `ConnectedComponents.run(graph)`, which computes Weakly Connected Components.
3. **Strongly Connected Component (SCC):** A directed graph is *strongly connected* if, for every pair of vertices $u$ and $v$, there exists a directed path from $u$ to $v$ AND a directed path from $v$ to $u$.

#### The Symmetrical Reciprocity Theorem
> **Theorem:** *If a directed graph $\mathcal{G} = (V, E)$ has $100\%$ reciprocity (meaning for every directed edge $(u, v) \in E$, the reverse edge $(v, u) \in E$ also exists), then the set of Strongly Connected Components is mathematically identical to the set of Weakly Connected Components.*

**Proof:**
1. Let $\mathcal{G}$ be a directed graph where $(u, v) \in E \iff (v, u) \in E$.
2. In such a graph, if there is a directed path $P = (v_1, v_2, \dots, v_k)$ from $v_1$ to $v_k$, then because every edge is reversible, the sequence $P^{-1} = (v_k, v_{k-1}, \dots, v_1)$ is also a valid directed path from $v_k$ to $v_1$.
3. Therefore, mutual directed reachability between any two nodes $u$ and $v$ is equivalent to undirected reachability.
4. Hence, $\text{WCC}(\mathcal{G}) = \text{SCC}(\mathcal{G})$. $\blacksquare$

**Empirical Application to Urbanix Day 7:**
Our checked-in dataset has 40 directed edges representing 20 bidirectional corridors (100% reciprocity). GraphX's `ConnectedComponents.run(graph)` computes exactly **1 weakly connected component**. By the Symmetrical Reciprocity Theorem, the Urbanix road network is **strictly strongly connected ($k=1$)**, proving that every intersection is reachable from every other intersection.

---

### 2.12 Transportation Systems Physics, Geodesy & Dynamic Travel Time Formulation
Transportation engineering links graph theory to physical kinematics and spatial geodesy.

#### 1. Geodetic Coordinates & The Haversine Distance Formula
Each intersection vertex in `data/intersections.csv` carries WGS-84 spherical coordinates: Latitude ($\phi$) and Longitude ($\lambda$). The great-circle distance $d_{\text{haversine}}$ between two GPS coordinates on Earth (mean radius $R \approx 6,371.0\text{ km}$) is computed as:
$$d = 2R \arcsin\left(\sqrt{\sin^2\left(\frac{\Delta \phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta \lambda}{2}\right)}\right)$$
In physical road networks, the actual road length ($d_{\text{base}}$) is always greater than or equal to the straight-line Haversine distance due to road curvature and terrain elevation ($d_{\text{base}} \ge d_{\text{haversine}}$).

#### 2. The Fundamental Diagram of Traffic Flow (Greenshields Model)
Traffic dynamics on any road segment follow the **Fundamental Equation of Traffic Flow**:
$$q = k \cdot v$$
Where:
- $q$: Traffic Flow rate (vehicles per hour, $\text{veh/h}$).
- $k$: Traffic Density (vehicles per kilometer, $\text{veh/km}$).
- $v$: Space Mean Speed ($\text{km/h}$).

As vehicle density $k$ approaches jam density $k_{\text{jam}}$, traffic velocity collapses toward zero ($v \to 0$), causing traffic flow to break down into gridlock.

#### 3. Dynamic Travel Time Formulation for Day 8
In Day 7, the static road weight is `base_distance_km` ($d_e$). In Day 8, edge weights will transition to **Estimated Travel Time** ($t_e$):
$$t_e = \frac{d_e}{\max(v_e, 5.0)}$$
Where:
- $d_e$: Physical segment length in kilometers.
- $v_e$: Predicted traffic speed in $\text{km/h}$ produced by Weekend 2's ML models for the corresponding zone.
- $\max(v_e, 5.0)$: A physical safety floor preventing division by zero or infinite travel times during severe traffic standstills.

---

## 🔄 3. Python to Scala Rosetta Stone (Zero-Assumptions Guide)

For an engineer transitioning from Python/PySpark, Scala can initially feel foreign due to its static typing and functional syntax. However, both languages express identical functional transformations.

| Functional Concept | Python / PySpark | Native Scala / Spark | Pedagogical Explanation |
| :--- | :--- | :--- | :--- |
| **Immutable Value** | `x = 10` (convention: `CONST = 10`) | `val x: Int = 10` | **Permanent Marker.** Once assigned, `val` can never be reassigned. The compiler optimizes it for multithreaded safety. |
| **Mutable Variable** | `x = 10; x = 20` | `var x: Int = 10; x = 20` | **Pencil with Eraser.** Can be overwritten. Idiomatic Scala strictly discourages `var` to avoid race conditions. |
| **Immutable Data Record**| `@dataclass(frozen=True)`<br>`class Node:`<br>`    id: int`<br>`    name: str` | `case class Node(`<br>`  id: Long,`<br>`  name: String`<br>`)` | Generates constructor, getters, immutable copy, `equals()`, `hashCode()`, and `toString()` methods automatically at compile time. |
| **Absence of Value** | `x = None` or `Optional[str]` | `val x: Option[String] = None`<br>`val y: Option[String] = Some("val")` | A container representing presence (`Some(v)`) or absence (`None`). Eliminates `NullPointerException` by forcing compile-time handling. |
| **Side-Effect Function** | `def log_data() -> None:` | `def logData(): Unit = { ... }` | `Unit` denotes a function returning nothing of value (like `void` in Java or `None` in Python), executed for side effects. |
| **Pattern Matching** | `match val:`<br>`  case 1: ...`<br>`  case _: ...` | `val match {`<br>`  case 1 => ...`<br>`  case _ => ...`<br>`}` | An exhaustive, compiler-checked structural switch statement capable of destructuring case classes and tuples safely. |
| **Anonymous Function** | `lambda x: x * 2` | `(x: Int) => x * 2` or `_ * 2` | First-class functional closure. The underscore `_` serves as a placeholder for a single argument. |
| **Collection Map** | `[x * 2 for x in items]` | `items.map(x => x * 2)` | Transforms every element of a collection by applying a pure function. |
| **Collection Filter** | `[x for x in items if x > 0]` | `items.filter(x => x > 0)` | Selects only those elements satisfying a boolean predicate function. |
| **Singleton Object** | Global module or class with `@staticmethod` | `object RoadGraphBuilder { ... }` | Creates a thread-safe Singleton instance lazily initialized by the JVM. Contains static helper methods and `main()`. |
| **Path-Dependent Import**| `from pyspark.sql.functions import *` | `val spark = ...`<br>`import spark.implicits._` | Imports implicit conversions (e.g., converting Scala Tuples to DataFrames via `.toDF()`). Requires `spark` to be an immutable `val`. |
| **Build Dependency** | `requirements.txt`: `pyspark==3.5.0` | `build.sbt`: `"org.apache.spark" %% "spark-core" % "3.5.0"` | The `%%` operator automatically appends the compiler binary suffix (e.g., `_2.12`) to the artifact name. |

---

## 🛠️ 4. Build System, JVM Toolchain & Binary Compatibility Architecture

### 4.1 The JVM Execution Engine & Memory Architecture
When running distributed graph computations, Apache Spark executes inside the **Java Virtual Machine (JVM)** across driver and executor processes:

```
+───────────────────────────────────────────────────────────────────────────+
|                           JVM Process Memory Layout                       |
|                                                                           |
|  ┌─────────────────────────────────────────────────────────┐  ┌────────┐  |
|  │                       JVM Heap                          │  │Metaspace│  |
|  │  ┌───────────────────────┐  ┌────────────────────────┐  │  │(Classes)│  |
|  │  │  Young Generation     │  │   Old Generation       │  │  └────────┘  |
|  │  │  (Eden + Survivors)   │  │   (Long-lived RDDs)    │  │  ┌────────┐  |
|  │  └───────────────────────┘  └────────────────────────┘  │  │Off-Heap │  |
|  │  ┌───────────────────────────────────────────────────┐  │  │(Netty,  │  |
|  │  │          Spark Unified Memory Pool (60%)          │  │  │ Unroll) │  |
|  │  │   [ Execution Memory ]  │   [ Storage Memory ]    │  │  └────────┘  |
|  │  │   (Shuffles, Aggs)      │   (Cached Graphs, RDDs) │  │              |
|  │  └───────────────────────────────────────────────────┘  │              |
|  └─────────────────────────────────────────────────────────┘              |
+───────────────────────────────────────────────────────────────────────────+
```

1. **Young Generation (Eden & Survivor Spaces):** Short-lived objects (e.g., intermediate tuple closures created during `rdd.map`) are allocated here. Fast Minor GC cycles collect them rapidly.
2. **Old Generation (Tenured Space):** Long-lived objects (such as cached GraphX `VertexRDD` and `EdgeRDD` partitions) are promoted here.
3. **Spark Unified Memory Manager:** Spark allocates $60\%$ of total heap to unified execution and storage memory:
   - **Storage Memory:** Holds cached graph partitions and routing tables.
   - **Execution Memory:** Holds shuffle buffers, message queues during Pregel supersteps, and aggregation hash maps.
4. **Metaspace:** Stores class metadata, bytecode, and reflection data outside the JVM heap.

---

### 4.2 The Scala Binary ABI Compatibility Rule
In the Python ecosystem, a package compiled for Python 3.9 can often run seamlessly in Python 3.10. 
In the Scala and JVM ecosystem, **binary interface (ABI) compatibility is strictly guaranteed only across point releases of the same minor version**:
- Bytecode compiled with Scala `2.12.18` can link with libraries built for Scala `2.12.0` through `2.12.19`.
- Bytecode compiled with Scala `2.12.x` **cannot** link with Scala `2.13.x` libraries. Attempting to link across minor versions triggers fatal runtime JVM errors:
  ```text
  java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)...
  java.lang.IncompatibleClassChangeError: ...
  ```

---

### 4.3 SBT Build Lifecycle, Task Graph & Incremental Compilation (Zinc)
`sbt` models software builds as a **Directed Acyclic Graph (DAG) of Tasks and Settings**:
1. **Settings vs. Tasks:**
   - **Setting:** A value evaluated once at project load time (e.g., `scalaVersion := "2.12.18"`, `name := "urbanix-graphx-reroute"`).
   - **Task:** A computation executed on demand every time it is triggered (e.g., `compile`, `test`, `run`).
2. **Zinc Incremental Compilation:**
   Instead of re-compiling all source files on every build (like traditional `scalac`), `sbt` uses **Zinc**:
   - Analyzes class dependency trees at the symbol level.
   - If only `RoadGraphBuilderSpec.scala` is modified, Zinc recompiles only the test class without touching `RoadGraphBuilder.scala`, dropping compilation times from 20 seconds to 2 seconds.
3. **Configuration Scopes:**
   - `Compile`: Production sources under `src/main/scala/`.
   - `Test`: Test sources under `src/test/scala/`.
   - `Runtime`: Runtime classpath dependencies.

---

### 4.4 Apache Spark 3.5.0 Binary Matrix & Local Distribution Isolation
The official Apache Spark 3.5.0 pre-built binaries published to Maven Central are compiled against **Scala 2.12**. Therefore, Urbanix strictly pins:
- Compiler: `Scala 2.12.18`
- Build Tool: `sbt 1.9.9` (pinned in `project/build.properties`)
- Java Virtual Machine: `Oracle JDK 1.8.0_202` (64-bit)

> [!CAUTION]
> **Local Spark Installation Warning:** The local host environment contains an unmanaged Spark distribution at `C:\spark\spark-3.5.9-bin-hadoop3` compiled against Scala 2.13. **Do not execute `spark-submit` using this local binary for Day 7**, as doing so mixes Scala 2.13 with our Scala 2.12 GraphX code. All compilation, testing, and execution must occur strictly through `sbt.bat`.

---

### 4.5 Annotated `services/graphx-reroute/build.sbt`
```scala
name := "urbanix-graphx-reroute"
version := "0.1.0"

// Pin Scala 2.12.18 to match Spark 3.5.0 pre-built binaries on Maven Central
scalaVersion := "2.12.18"

// The '%%' automatically appends '_2.12' to artifact names
libraryDependencies ++= Seq(
  // Core Spark runtime: RDD abstraction, cluster scheduling, memory management
  "org.apache.spark" %% "spark-core"   % "3.5.0",

  // Spark SQL: DataFrame API, Catalyst optimizer, CSV reader
  "org.apache.spark" %% "spark-sql"    % "3.5.0",

  // Spark GraphX: Property graph engine, VertexRDD, EdgeRDD, Pregel algorithms
  "org.apache.spark" %% "spark-graphx" % "3.5.0",

  // ScalaTest: Unit and integration testing framework (scoped to Test only)
  "org.scalatest"    %% "scalatest"    % "3.2.18" % Test
)

// Fork separate JVM for 'run' and 'test' to prevent Spark context leakage
```

---

## 🗺️ 5. Dataset Design, Geospatial Topology & Provenance

### 5.1 Distinguishing Sensor Zones from Graph Intersections
A common source of confusion in smart city architectures is the difference between sensor zones and graph vertices:
1. **Sensor Zones (`zone_1` to `zone_5`):** Coarse spatial polygons (e.g., Downtown Core, San Fernando Valley). Streaming IoT feeds (traffic loop detectors, EPA air quality monitors, OpenWeather stations) are aggregated at the zone level. In Weekend 2, MLlib trained models that predict traffic speed for each zone.
2. **Graph Vertices (Nodes 1 to 20):** Discrete physical intersections (e.g., Grand Ave & 1st St). Vehicles travel between physical intersections along road corridors.

Each intersection belongs to exactly one zone (`zone_id`), establishing the spatial join key that Day 8 will use to assign predicted zone speeds to specific road segments:
$$\text{Intersection } v \in \text{Zone } z$$

### 5.2 Topology Provenance: Synthetic Demonstration Network
The road network defined in `data/intersections.csv` and `data/roads.csv` is a **synthetic demonstration topology** designed specifically for Urbanix:
- It models inter-zone arterial grids and major freeway spines connecting the five Los Angeles operational zones.
- The geographic coordinates and road distances are illustrative and engineered to test distributed graph algorithms, edge cases, and rerouting behavior.
- It must not be cited as an authoritative municipal GIS land survey.

### 5.3 Complete Master Datasets

#### Intersections Master (`data/intersections.csv` — 20 Rows)
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

#### Road Segments Master (`data/roads.csv` — 40 Rows)
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

## 💻 6. Source Code Deep-Dive (`RoadGraphBuilder.scala`)

`services/graphx-reroute/src/main/scala/RoadGraphBuilder.scala` is the production engine of Day 7. It implements a two-phase validation and construction architecture.

### 6.1 Data Structures: `SourceValidationReport` & `GraphSummary`
```scala
case class SourceValidationReport(
  sourceIntersectionCount: Long,
  distinctIntersectionIdCount: Long,
  duplicateIntersectionIdCount: Long,
  nullOrInvalidNodeIdCount: Long,
  blankIntersectionNameCount: Long,
  sourceRoadCount: Long,
  roadsExactDuplicateRecordCount: Long,
  roadsInvalidEndpointCount: Long,
  roadsInvalidDistanceCount: Long,
  roadsSelfLoopCount: Long,
  isSourceValid: Boolean,
  errorMessages: Seq[String]
)

case class GraphSummary(
  sourceVertexCount: Long,
  graphVertexCount: Long,
  sourceEdgeCount: Long,
  graphEdgeCount: Long,
  danglingEdgeCount: Long,
  phantomVertexCount: Long,
  selfLoopCount: Long,
  weaklyConnectedComponentCount: Long,
  isDirectedSymmetric: Boolean,
  isValid: Boolean,
  diagnostics: Seq[String]
)
```
- `SourceValidationReport`: Captures pre-construction metrics before any RDD conversion occurs.
- `GraphSummary`: Captures post-construction topological audit results, component connectivity, and directional symmetry.

### 6.2 Phase 1: Pre-Construction Source Data Validation (`validateSourceData`)
```scala
def validateSourceData(intersectionsDF: DataFrame, roadsDF: DataFrame): SourceValidationReport = {
  // 1. Audit Intersections
  val totalIntersections = intersectionsDF.count()
  val validNodeIdDF = intersectionsDF.filter(col("id").isNotNull && col("id") > 0)
  val nullOrInvalidNodeIds = totalIntersections - validNodeIdDF.count()

  val distinctIds = intersectionsDF.select(col("id").cast(LongType)).distinct().count()
  val duplicateIds = totalIntersections - distinctIds

  val blankNames = intersectionsDF.filter(
    col("name").isNull || trim(col("name")) === ""
  ).count()

  // 2. Audit Road Segments
  val totalRoads = roadsDF.count()
  
  // Exact duplicate detection: distinguish legitimate reverse edges (u,v) vs duplicate records (u,v)+(u,v)
  val exactDuplicateRoads = roadsDF.groupBy("src_id", "dst_id").count().filter(col("count") > 1).count()

  // Valid endpoints check against valid node IDs
  val validNodeIds = intersectionsDF.select(col("id").cast(LongType)).rdd.map(_.getLong(0)).collect().toSet
  val invalidEndpoints = roadsDF.filter(row => {
    val src = if (row.isNullAt(row.fieldIndex("src_id"))) -1L else row.getAs[Number]("src_id").longValue()
    val dst = if (row.isNullAt(row.fieldIndex("dst_id"))) -1L else row.getAs[Number]("dst_id").longValue()
    !validNodeIds.contains(src) || !validNodeIds.contains(dst)
  }).count()

  // Invalid distance check: null, <= 0, NaN, or Infinite
  val invalidDistances = roadsDF.filter(
    col("base_distance_km").isNull ||
    col("base_distance_km") <= 0.0 ||
    isnan(col("base_distance_km")) ||
    col("base_distance_km") === lit(Double.PositiveInfinity) ||
    col("base_distance_km") === lit(Double.NegativeInfinity)
  ).count()

  // Self-loops check: src_id == dst_id
  val selfLoops = roadsDF.filter(col("src_id") === col("dst_id")).count()

  val errors = scala.collection.mutable.ArrayBuffer[String]()
  if (duplicateIds > 0) errors += s"Found $duplicateIds duplicate intersection IDs."
  if (nullOrInvalidNodeIds > 0) errors += s"Found $nullOrInvalidNodeIds null/invalid node IDs."
  if (blankNames > 0) errors += s"Found $blankNames blank/null intersection names."
  if (exactDuplicateRoads > 0) errors += s"Found $exactDuplicateRoads duplicate road records."
  if (invalidEndpoints > 0) errors += s"Found $invalidEndpoints road segments with missing endpoints."
  if (invalidDistances > 0) errors += s"Found $invalidDistances road segments with non-positive, null, NaN, or infinite distance."
  if (selfLoops > 0) errors += s"Found $selfLoops self-loop road segments."

  SourceValidationReport(
    totalIntersections, distinctIds, duplicateIds, nullOrInvalidNodeIds, blankNames,
    totalRoads, exactDuplicateRoads, invalidEndpoints, invalidDistances, selfLoops,
    isSourceValid = errors.isEmpty,
    errorMessages = errors
  )
}
```
**Pedagogical Highlights:**
1. **Handling Infinite Distances in Spark SQL:** Standard Spark SQL does not provide an `isinf()` column function. We test for infinity using `col("base_distance_km") === lit(Double.PositiveInfinity)`.
2. **Directed Duplicate Isolation:** Symmetrical edges `(1, 2)` and `(2, 1)` have different column values under `src_id` and `dst_id`. Grouping by `("src_id", "dst_id")` ensures that legitimate bidirectional pairs are never mistakenly flagged as duplicates.

### 6.3 Phase 2: GraphX Construction (`buildGraph`)
```scala
def buildGraph(spark: SparkSession, intersectionsDF: DataFrame, roadsDF: DataFrame): Graph[String, Double] = {
  // Map intersections to VertexRDD[(VertexId, String)]
  val vertices: RDD[(VertexId, String)] = intersectionsDF.rdd.map { row =>
    val id = row.getAs[Number]("id").longValue()
    val name = if (row.isNullAt(row.fieldIndex("name"))) s"Intersection_$id" else row.getAs[String]("name")
    (id, name)
  }

  // Map roads to EdgeRDD[Edge[Double]]
  val edges: RDD[Edge[Double]] = roadsDF.rdd.map { row =>
    val srcId = row.getAs[Number]("src_id").longValue()
    val dstId = row.getAs[Number]("dst_id").longValue()
    val distance = row.getAs[Number]("base_distance_km").doubleValue()
    Edge(srcId, dstId, distance)
  }

  Graph(vertices, edges)
}
```

### 6.4 Phase 3: Graph Topology & Structural Audit (`auditGraph`)
```scala
def auditGraph(graph: Graph[String, Double], intersectionsDF: DataFrame, roadsDF: DataFrame): GraphSummary = {
  val expectedVertexCount = intersectionsDF.count()
  val expectedEdgeCount = roadsDF.count()

  val graphVertexCount = graph.vertices.count()
  val graphEdgeCount = graph.edges.count()

  // Collect source vertex IDs directly from source DataFrame
  val validSourceIds = intersectionsDF.select(col("id").cast(LongType)).rdd.map(_.getLong(0)).collect().toSet

  // Identify phantom vertices auto-inserted by GraphX for missing endpoints
  val phantomVertices = graph.vertices.filter { case (id, attr) =>
    attr == null || !validSourceIds.contains(id)
  }.count()

  // Identify dangling edges
  val danglingEdges = graph.edges.filter(e =>
    !validSourceIds.contains(e.srcId) || !validSourceIds.contains(e.dstId)
  ).count()

  // Identify self-loops
  val selfLoops = graph.edges.filter(e => e.srcId == e.dstId).count()

  // Compute weakly connected components
  val weaklyConnectedComponents = if (graphVertexCount > 0) {
    ConnectedComponents.run(graph).vertices.map(_._2).distinct().count()
  } else 0L

  // Compute directed symmetry (reciprocity)
  val directedPairs = graph.edges.map(e => (e.srcId, e.dstId)).collect().toSet
  val isSymmetric = directedPairs.nonEmpty && directedPairs.forall { case (u, v) =>
    directedPairs.contains((v, u))
  }

  val diagnostics = scala.collection.mutable.ArrayBuffer[String]()
  if (graphVertexCount != expectedVertexCount) {
    diagnostics += s"Vertex count mismatch: source=$expectedVertexCount, graph=$graphVertexCount"
  }
  if (graphEdgeCount != expectedEdgeCount) {
    diagnostics += s"Edge count mismatch: source=$expectedEdgeCount, graph=$graphEdgeCount"
  }
  if (phantomVertices > 0) {
    diagnostics += s"Graph contains $phantomVertices phantom vertices created for unregistered endpoints"
  }
  if (danglingEdges > 0) {
    diagnostics += s"Graph contains $danglingEdges dangling edges with missing endpoints"
  }
  if (selfLoops > 0) {
    diagnostics += s"Graph contains $selfLoops self-loop edges (srcId == dstId)"
  }

  val isValid = diagnostics.isEmpty && phantomVertices == 0 && danglingEdges == 0 && selfLoops == 0

  GraphSummary(
    sourceVertexCount = expectedVertexCount,
    graphVertexCount = graphVertexCount,
    sourceEdgeCount = expectedEdgeCount,
    graphEdgeCount = graphEdgeCount,
    danglingEdgeCount = danglingEdges,
    phantomVertexCount = phantomVertices,
    selfLoopCount = selfLoops,
    weaklyConnectedComponentCount = weaklyConnectedComponents,
    isDirectedSymmetric = isSymmetric,
    isValid = isValid,
    diagnostics = diagnostics
  )
}
```

### 6.5 Production Entry Point: `validateAndBuildGraph`
```scala
def validateAndBuildGraph(
  spark: SparkSession,
  intersectionsDF: DataFrame,
  roadsDF: DataFrame,
  strict: Boolean = true
): (Graph[String, Double], GraphSummary) = {
  // Step 1: Pre-construction source data validation
  val sourceReport = validateSourceData(intersectionsDF, roadsDF)
  if (strict && !sourceReport.isSourceValid) {
    throw new IllegalArgumentException(
      s"Source validation failed with ${sourceReport.errorMessages.size} error(s):\n" +
      sourceReport.errorMessages.mkString("\n - ", "\n - ", "")
    )
  }

  // Step 2: Graph construction
  val graph = buildGraph(spark, intersectionsDF, roadsDF)

  // Step 3: Graph topology audit
  val summary = auditGraph(graph, intersectionsDF, roadsDF)
  if (strict && !summary.isValid) {
    throw new IllegalStateException(
      s"Graph audit failed with ${summary.diagnostics.size} diagnostic(s):\n" +
      summary.diagnostics.mkString("\n - ", "\n - ", "")
    )
  }

  (graph, summary)
}
```

---

## 🧪 7. Automated Test Suite Deep-Dive (`RoadGraphBuilderSpec.scala`)

The test suite contains **13 unit and integration tests** in `services/graphx-reroute/src/test/scala/RoadGraphBuilderSpec.scala`. Every test executes against actual production methods:

### Detailed Breakdown of the 13 Tests

| Test # | Test Name | Target Production Method | Verification Logic & Edge Cases Exercised |
| :---: | :--- | :--- | :--- |
| **1** | Basic Construction | `RoadGraphBuilder.buildGraph` | Constructs a 3-node, 2-edge fixture; asserts `graph.vertices.count() == 3` and `graph.edges.count() == 2`. |
| **2** | Deterministic ID Mapping | `RoadGraphBuilder.buildGraph` | Runs construction twice; asserts vertex IDs match source data and `ids1.sameElements(ids2)` is true. |
| **3** | Duplicate ID Detection | `RoadGraphBuilder.validateSourceData` | Injects duplicate intersection ID `1L`; asserts `sourceReport.duplicateIntersectionIdCount == 1` and `isSourceValid == false`. |
| **4** | Dangling Edge & Phantom Nodes | `RoadGraphBuilder.auditGraph` | Injects road edge targeting nonexistent node `999L`; asserts `summary.danglingEdgeCount == 1`, `summary.phantomVertexCount == 1`, and `isValid == false`. |
| **5** | Invalid Edge Distances | `RoadGraphBuilder.validateSourceData` | Injects negative (`-1.0`), zero (`0.0`), and `NaN` distances; asserts `sourceReport.roadsInvalidDistanceCount == 3` and `isSourceValid == false`. |
| **6** | Directionality Preservation | `RoadGraphBuilder.buildGraph` | Injects one-way directed edge `1L -> 2L`; asserts forward edge count is 1 and reverse edge `2L -> 1L` count is strictly 0. |
| **7** | Self-Loop Detection | `validateSourceData` & `auditGraph` | Injects self-loop edge `1L -> 1L`; asserts both pre-construction and post-construction audits flag `selfLoopCount == 1` and fail validity. |
| **8** | Empty Input Handling | `RoadGraphBuilder.buildGraph` | Passes empty DataFrames; asserts GraphX safely returns 0 vertices and 0 edges without exceptions. |
| **9** | Count Reconciliation | `validateAndBuildGraph` | Reconciles source DataFrame rows against GraphX counts on a 4-node, 6-edge fixture; asserts `summary.isValid == true`. |
| **10**| Native GraphX Operators | `triplets`, `inDegrees`, `outDegrees`, `subgraph` | Exercises `graph.triplets` (inspects src/dst attributes), verifies degree counts, and applies `subgraph(epred = _.attr < 5.0)`. |
| **11**| Strict Fail-Fast Policy | `validateAndBuildGraph(..., strict=true)` | Injects corrupt negative distance data; verifies `intercept[IllegalArgumentException]` is thrown. |
| **12**| Duplicates vs. Bidirectional | `RoadGraphBuilder.validateSourceData` | Tests that legitimate reverse edge `(2, 1)` is accepted as valid, whereas duplicate directed record `(1, 2) + (1, 2)` is rejected. |
| **13**| Verified Checked-In Topology | `validateAndBuildGraph` on real CSVs | Loads `data/intersections.csv` and `data/roads.csv`; verifies 20 vertices, 40 edges, 0 dangling, 0 phantoms, 1 connected component, and 100% symmetry. |

---

## ⚡ 8. Interactive Spark-Shell Exploration Playbook

For hands-on demonstration, debugging, or examiner inspection during a viva defense, you can launch `spark-shell` and explore the graph interactively using this complete, copy-pasteable Scala script:

```scala
// ============================================================================
// URBANIX DAY 7: INTERACTIVE SPARK-SHELL REPL EXPLORATION
// Launch: spark-shell --packages org.apache.spark:spark-graphx_2.12:3.5.0
// ============================================================================

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// 1. Load Source Datasets
val intersectionsDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/intersections.csv")
val roadsDF = spark.read.option("header", "true").option("inferSchema", "true").csv("data/roads.csv")

println(s"Source Rows: ${intersectionsDF.count()} intersections, ${roadsDF.count()} roads")

// 2. Build GraphX Vertex and Edge RDDs
val vertices: RDD[(VertexId, String)] = intersectionsDF.rdd.map { row =>
  (row.getAs[Number]("id").longValue(), row.getAs[String]("name"))
}

val edges: RDD[Edge[Double]] = roadsDF.rdd.map { row =>
  val src = row.getAs[Number]("src_id").longValue()
  val dst = row.getAs[Number]("dst_id").longValue()
  val dist = row.getAs[Number]("base_distance_km").doubleValue()
  Edge(src, dst, dist)
}

val graph = Graph(vertices, edges)

// 3. Inspect Graph Properties
println(s"Graph Vertices: ${graph.vertices.count()}")
println(s"Graph Edges:    ${graph.edges.count()}")

// 4. Inspect Edge Triplets
println("\nSample Edge Triplets:")
graph.triplets.take(5).foreach { t =>
  println(f"[Vertex ${t.srcId}%2d: ${t.srcAttr}%-28s] --(${t.attr}%4.1f km)--> [Vertex ${t.dstId}%2d: ${t.dstAttr}%-28s]")
}

// 5. Inspect Degree Distributions
println("\nTop Intersections by Out-Degree (Major Interchanges):")
graph.outDegrees.join(graph.vertices).sortBy(_._2._1, ascending = false).take(5).foreach {
  case (id, (deg, name)) => println(f"Vertex $id%2d ($name%-28s): Out-Degree = $deg")
}

// 6. Verify Connected Components
import org.apache.spark.graphx.lib.ConnectedComponents
val ccGraph = ConnectedComponents.run(graph)
val componentCount = ccGraph.vertices.map(_._2).distinct().count()
println(s"\nWeakly Connected Components: $componentCount (1 = Fully Connected)")

// 7. Verify Directed Symmetry (Reciprocity)
val edgePairs = graph.edges.map(e => (e.srcId, e.dstId)).collect().toSet
val is100PercentReciprocal = edgePairs.forall { case (u, v) => edgePairs.contains((v, u)) }
println(s"100% Reciprocal Bidirectional Corridors: $is100PercentReciprocal")

// 8. Filter Short Road Segments (Subgraph)
val localStreets = graph.subgraph(epred = triplet => triplet.attr < 2.0)
println(s"Short Local Arterials (< 2.0 km): ${localStreets.edges.count()} edges")
```

---

## 💻 9. Step-by-Step Terminal Execution Commands

All commands are executed from `services/graphx-reroute/` using PowerShell:

```powershell
# Navigate to the Scala GraphX module
cd services/graphx-reroute

# 1. Compile production and test Scala code
.\sbt.bat --batch compile

# 2. Execute the full 13-test automated verification suite
.\sbt.bat --batch test

# 3. Run the production RoadGraphBuilder application on checked-in CSVs
.\sbt.bat --batch run

# 4. Run historical multi-day regression suite from repository root
cd ../..
python -m pytest services/kafka/tests/
python -m pytest services/streaming/tests/test_streaming_quality.py
python -m pytest services/streaming/tests/test_feature_pipeline.py
python -m pytest services/streaming/tests/test_model_training.py
python -m pytest services/streaming/tests/test_streaming_inference.py
python services/streaming/verify_models.py
python services/streaming/verify_split.py
```

---

## 📊 10. Empirical Execution Logs & Count Reconciliation

### 10.1 Scala Compilation Log (`sbt compile`)
```text
[info] welcome to sbt 1.9.9 (Oracle Corporation Java 1.8.0_202)
[info] loading project definition from ...\services\graphx-reroute\project
[info] loading settings for project graphx-reroute from build.sbt ...
[info] set current project to urbanix-graphx-reroute
[info] compiling 1 Scala source to ...\services\graphx-reroute\target\scala-2.12\classes ...
[info] done compiling
[success] Total time: 8 s, completed 21 Oct, 2026 10:05:08 AM
```

### 10.2 Automated Scala Test Suite Log (`sbt test` — 13 Tests)
```text
[info] welcome to sbt 1.9.9 (Oracle Corporation Java 1.8.0_202)
[info] set current project to urbanix-graphx-reroute
[info] compiling 1 Scala source to ...\target\scala-2.12\test-classes ...
[info] done compiling
[info] RoadGraphBuilderSpec:
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
26/10/21 10:05:32 INFO SparkContext: Running Spark version 3.5.0
26/10/21 10:05:32 INFO SparkContext: Java version 1.8.0_202
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

### 10.3 RoadGraphBuilder Main Application Run Log (`sbt run`)
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
[info] ----------------------------------------------------------------------
[info] PHASE 2: GRAPHX PROPERTY GRAPH CONSTRUCTION & AUDIT
[info] ----------------------------------------------------------------------
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
[info] ======================================================================
[info] FINAL VERIFICATION: [PASS] GRAPH SIZE & TOPOLOGY MATCHES SOURCE DATA EXACTLY
[info] Road network graph is verified and ready for Day 8 shortest-path routing.
[info] ======================================================================
[success] Total time: 19 s, completed 21 Oct, 2026 10:06:21 AM
```

### 10.4 Complete Count Reconciliation Matrix
| Metric | Source CSV Rows | GraphX RDD Count | Match Status | Verification Mechanism |
| :--- | :---: | :---: | :---: | :--- |
| **Intersections / Vertices** | 20 | 20 | **PERFECT MATCH** | `graph.vertices.count() == intersectionsDF.count()` |
| **Road Segments / Edges** | 40 | 40 | **PERFECT MATCH** | `graph.edges.count() == roadsDF.count()` |
| **Duplicate Intersection IDs** | 0 | 0 | **PERFECT MATCH** | `totalIntersections - distinctIds == 0` |
| **Missing Endpoints (Dangling)**| 0 | 0 | **PERFECT MATCH** | Validated against source vertex ID set |
| **Phantom Vertices** | 0 | 0 | **PERFECT MATCH** | Vertices with `attr == null` count == 0 |
| **Self-Loops (`src == dst`)** | 0 | 0 | **PERFECT MATCH** | Filtered `e.srcId == e.dstId` count == 0 |
| **Connected Components** | 1 | 1 | **PERFECT MATCH** | `ConnectedComponents.run(graph)` distinct count == 1 |
| **Directed Symmetry** | 100% | 100% | **PERFECT MATCH** | $(u, v) \in E \implies (v, u) \in E$ for all 40 edges |

---

## 🛡️ 11. Multi-Day Regression Matrix & Invariance Proof

All historical test suites and invariant verifiers across Days 2–7 were executed:

| Module / Milestone | Executed Test Command | Tests Run | Tests Passed | Execution Time | Status |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Day 2: Producers** | `python -m pytest services/kafka/tests/` | 19 | 19 | 0.57s | **PASS** |
| **Day 3: Ingestion** | `python -m pytest services/streaming/tests/test_streaming_quality.py` | 14 | 14 | 61.95s | **PASS** |
| **Day 4: Features** | `python -m pytest services/streaming/tests/test_feature_pipeline.py` | 11 | 11 | 60.45s | **PASS** |
| **Day 5: MLlib Models** | `python -m pytest services/streaming/tests/test_model_training.py` | 9 | 9 | 81.29s | **PASS** |
| **Day 6: Live Inference**| `python -m pytest services/streaming/tests/test_streaming_inference.py` | 10 | 10 | 40.98s | **PASS** |
| **Day 7: GraphX Engine** | `.\sbt.bat --batch test` | 13 | 13 | 22.11s | **PASS** |
| **TOTAL REPOSITORY** | — | **76** | **76** | **~4m 27s** | **ZERO REGRESSION** |

### Data & Model Invariance Proof
1. `services/streaming/verify_models.py`: Passed all 12 checks. Native `LinearRegressionModel` at `models/linear_regression` remains intact ($R^2 = 0.7757$, $\text{RMSE} = 2.9280\text{ km/h}$).
2. `services/streaming/verify_split.py`: Passed all checks. Train rows = 365, Test rows = 100, zero temporal leakage, target strictly excluded from feature vectors.

---

## 🚧 12. Scope Boundaries & Day 8 Integration Interface

Day 7 strictly limits its scope to static graph construction, schema validation, and topology auditing.

### 12.1 What Day 7 Solved
1. Initialized the Scala 2.12.18 / sbt 1.9.9 build environment.
2. Pinned Apache Spark GraphX 3.5.0 dependencies.
3. Created the two-phase validation architecture (`validateSourceData`, `buildGraph`, `auditGraph`).
4. Reconciled and validated 20 intersections and 40 directed road segments.
5. Proved 100% reciprocity and strong connectivity.
6. Expanded automated testing to 13 production tests.

### 12.2 What Day 8 Will Implement (Strict Boundary)
1. **Dynamic Congestion Weights:** Transitioning from static distance ($d_e$) to dynamic travel time ($t_e$):
   $$t_e = \frac{d_e}{\max(v_e, 5.0)}$$
   Where $v_e$ is the real-time speed prediction in $\text{km/h}$ produced by the Day 5/6 MLlib streaming pipeline for the road segment's zone.
2. **Shortest Path Computation:** Calling GraphX's `Pregel` or `ShortestPaths` API to calculate the minimum travel-time route from an origin intersection to a destination.
3. **Dynamic Detour Simulation:** Simulating a sudden traffic speed collapse on US-101 (e.g., from $95\text{ km/h}$ to $12\text{ km/h}$) and verifying that the routing algorithm diverts traffic via CA-110 or local arterials.

---

## 🎓 13. Comprehensive Academic Viva Voce Defense Playbook (40 Questions & Answers)

### Section A: Scala Syntax & Language Fundamentals (Q1–Q10)

#### Q1: What is Scala, and why does Urbanix use it instead of Python for Day 7?
**Answer:** Scala is a statically-typed, functional and object-oriented programming language that compiles directly to Java Virtual Machine (JVM) bytecode. Urbanix uses Scala for Day 7 because **Apache Spark GraphX is exclusively a native JVM/Scala API**. PySpark provides no bindings for GraphX; graph message passing and pointer manipulation across the Py4J socket bridge would incur catastrophic serialization overhead.

#### Q2: What is the exact difference between `val` and `var` in Scala?
**Answer:** `val` creates an immutable value binding whose reference can never be reassigned once initialized (similar to `final` in Java). `var` creates a mutable variable that can be reassigned. Idiomatic Scala strongly enforces `val` to guarantee thread safety, eliminate concurrency race conditions, and enable compiler optimizations.

#### Q3: Why does `import spark.implicits._` require `spark` to be a `val`?
**Answer:** In Scala, path-dependent imports require a *stable identifier*—an immutable symbol whose reference is guaranteed never to change at runtime. If `spark` were declared as a `var`, its reference could change dynamically, creating ambiguous import scopes and compiler errors.

#### Q4: What is a `case class` in Scala, and what is its Python equivalent?
**Answer:** A `case class` is an immutable data carrier. The Scala compiler automatically generates the constructor, public getters, structural `equals()`, `hashCode()`, `toString()`, pattern matching extractors (`unapply`), and a `.copy()` method. Its direct Python equivalent is `@dataclass(frozen=True)`.

#### Q5: What is an `object` in Scala, and how does it differ from a `class`?
**Answer:** A `class` is an instantiable template created via `new`. An `object` defines a **Singleton**—a single instance created lazily by the JVM upon its first reference. It is used to hold static utility methods, constants, and the application entry point `def main(args: Array[String]): Unit`.

#### Q6: What does the return type `Unit` signify in Scala?
**Answer:** `Unit` represents the absence of a meaningful return value. It is analogous to `void` in Java or returning `None` from a Python function that executes purely for side effects (such as printing to the console or persisting files to disk).

#### Q7: What is `Option[T]`, and how does it prevent runtime crashes compared to Python?
**Answer:** `Option[T]` is a sealed container with exactly two subtypes: `Some(value)` and `None`. Unlike Python, where any variable can silently be `None` (leading to unexpected `AttributeError: 'NoneType' object has no attribute...` at runtime), Scala forces the developer to handle both cases explicitly at compile time using pattern matching or `.getOrElse()`, completely eliminating `NullPointerException`.

#### Q8: What does the `%%` operator mean in `build.sbt`?
**Answer:** The double percent `%%` instructs sbt to automatically append the active Scala binary version (e.g., `_2.12`) to the artifact name when fetching dependencies from Maven Central (`spark-graphx_2.12-3.5.0.jar`). A single `%` uses the literal artifact name without appending Scala versions.

#### Q9: What is Pattern Matching in Scala?
**Answer:** Pattern matching (`expr match { case ... }`) is a compile-time verified construct for checking values against structural patterns, destructuring case classes and tuples, and extracting variables safely with exhaustive check warnings.

#### Q10: How does Scala's local type inference work?
**Answer:** The Scala compiler uses local type inference to deduce expression types from their context (e.g., `val x = 10` is inferred as `Int`). However, method signatures and public API boundaries must be explicitly typed to maintain clear software contracts.

---

### Section B: Build Tool (sbt) & Environment Compatibility (Q11–Q18)

#### Q11: What is `sbt`, and what role does it serve?
**Answer:** `sbt` (Scala Build Tool) is the standard build tool for Scala applications. It manages dependency resolution, downloads library jars from Maven Central, compiles Scala sources into `.class` bytecode files, packages artifacts into `.jar` archives, and executes automated test suites.

#### Q12: Why was Scala 2.12.18 specifically chosen rather than Scala 2.13 or 3.x?
**Answer:** The official Apache Spark 3.5.0 pre-built binaries on Maven Central are compiled against Scala 2.12. In Scala, minor versions are **not binary compatible**. Attempting to run Spark 3.5.0 with Scala 2.13 triggers fatal `NoSuchMethodError` and `IncompatibleClassChangeError` exceptions at JVM startup.

#### Q13: What does `fork := true` do in `build.sbt`?
**Answer:** It instructs sbt to spawn a separate, dedicated JVM process when running `sbt run` or `sbt test`. This guarantees that SparkSession singletons, static loggers, and Netty network buffers do not leak into the parent sbt interactive shell process.

#### Q14: What is the purpose of `project/build.properties`?
**Answer:** It pins the exact version of sbt (`sbt.version=1.9.9`), ensuring deterministic, reproducible builds across different development machines and CI/CD runners regardless of the launcher installed.

#### Q15: How does sbt download dependencies without requiring a pre-installed Scala compiler?
**Answer:** sbt is self-bootstrapping. It inspects `scalaVersion := "2.12.18"` in `build.sbt` and automatically downloads the exact Scala compiler and standard library jars into the local user cache (`~/.cache/coursier` or `~/.ivy2/`).

#### Q16: What happens during `sbt compile`?
**Answer:** sbt resolves dependencies, triggers the Zinc incremental compiler, analyzes source file dependencies, and compiles `.scala` files into JVM `.class` bytecode inside `target/scala-2.12/classes/`.

#### Q17: What does `% Test` mean in libraryDependencies?
**Answer:** It assigns the dependency (such as ScalaTest) strictly to the test scope. The library is available when running `sbt test`, but is excluded from production compilation and packaging.

#### Q18: What is the difference between `sbt run` and `sbt compile`?
**Answer:** `sbt compile` only type-checks and compiles source code into bytecode. `sbt run` compiles modified sources and then invokes the JVM to execute the `main()` method of the designated Singleton object.

---

### Section C: Graph Theory & Distributed GraphX Architecture (Q19–Q28)

#### Q19: What is a Property Graph in GraphX?
**Answer:** A directed multigraph with user-defined properties attached to each vertex and edge. Formally represented as `Graph[VD, ED]`, where `VD` is the vertex attribute type and `ED` is the edge attribute type.

#### Q20: What is a `VertexId` in GraphX, and what is its data type?
**Answer:** `VertexId` is a 64-bit signed integer (`scala.Long`). GraphX requires all vertex identifiers to be Longs for fast hashing, bitwise indexing, and internal array lookups.

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
**Answer:** GraphX uses **vertex-cut partitioning**: edges are evenly distributed across cluster partitions, while vertices that have edges on multiple partitions are replicated across those partitions with a **Vertex Routing Table**. This minimizes communication overhead for high-degree hub vertices.

#### Q24: What happens if an edge references a vertex that is not in the vertex RDD?
**Answer:** GraphX automatically inserts that missing vertex into `graph.vertices` with a default vertex attribute (`null.asInstanceOf[VD]`). Our production `auditGraph()` method explicitly checks for `attr == null` and cross-references source DataFrames to detect and flag these phantom vertices.

#### Q25: What is the difference between `inDegrees`, `outDegrees`, and `degrees`?
**Answer:**
- `outDegrees`: Number of directed edges leaving a vertex (`srcId == vertexId`).
- `inDegrees`: Number of directed edges entering a vertex (`dstId == vertexId`).
- `degrees`: Total number of adjacent edges (`inDegrees + outDegrees`).

#### Q26: What is the `subgraph` operator in GraphX?
**Answer:** `subgraph(epred, vpred)` returns a new graph containing only the vertices that satisfy the vertex predicate `vpred` and edges that satisfy the edge predicate `epred`.

#### Q27: Why doesn't GraphX use standard Spark DataFrames instead of RDDs?
**Answer:** GraphX was designed around RDDs before the DataFrame Catalyst optimizer existed. Graph algorithms require fine-grained pointer manipulation, array indexing, and iterative message passing that cannot easily be expressed as relational SQL plans.

#### Q28: What is the Bulk-Synchronous Parallel (BSP) model in Pregel?
**Answer:** A parallel computing model structured into synchronized iterations called **supersteps**. In each superstep, vertices receive messages from incoming edges, execute a local user-defined computation, send messages along outgoing edges, and optionally vote to halt. A global synchronization barrier separates each superstep.

---

### Section D: Urbanix Road Graph Model, Two-Phase Validation & Edge Cases (Q29–Q36)

#### Q29: How many intersections and road segments were constructed in Day 7?
**Answer:** Exactly 20 physical intersections (vertices) and 40 directed road segments (edges) representing 20 bidirectional corridors across all 5 Urbanix zones.

#### Q30: Why are roads represented as directed edges rather than undirected?
**Answer:** Real metropolitan traffic networks are inherently directed. Traffic flows independently in opposite directions; one direction of a highway or avenue can experience severe congestion while the reverse direction is free-flowing. Two directed edges (`u -> v` and `v -> u`) allow independent congestion weights in Day 8.

#### Q31: What does the initial edge attribute represent in Day 7?
**Answer:** In Day 7, the edge attribute represents `base_distance_km` ($ED = \text{Double}$), the physical length of the road segment in kilometers.

#### Q32: Why is post-construction dangling edge detection insufficient on its own?
**Answer:** GraphX auto-creates phantom vertices (`null.asInstanceOf[VD]`) for any edge referencing an unregistered vertex ID. If an audit only checks `graph.vertices.map(_._1)`, those phantom IDs are included, masking missing endpoints. Therefore, an independent pre-construction audit (`validateSourceData`) must validate the source DataFrames directly.

#### Q33: How does the system distinguish accidental duplicate records from valid bidirectional links?
**Answer:** A valid bidirectional corridor consists of two distinct directed edges: $(u \to v)$ and $(v \to u)$. Duplicate records occur when the exact directed pair $(u, v)$ appears multiple times. The production validator groups by `("src_id", "dst_id")` with `count > 1` to isolate true duplicate records while accepting reciprocal edge pairs.

#### Q34: What constitutes an invalid edge distance in our production validator?
**Answer:** Any distance that is `null`, less than or equal to `0.0`, `NaN`, `Double.PositiveInfinity`, or `Double.NegativeInfinity`.

#### Q35: How does Day 7 maintain determinism across repeated executions?
**Answer:** Vertex IDs in `intersections.csv` are explicitly numbered 1 through 20. No random ID generation or nondeterministic row-number hashing is used. Repeated runs produce bitwise identical VertexIds and Graph structures.

#### Q36: What is the strict fail-fast policy in `validateAndBuildGraph`?
**Answer:** When `strict = true` (the production default), any pre-construction source error throws `IllegalArgumentException`, and any post-construction audit failure throws `IllegalStateException`.

---

### Section E: Graph Algorithms, Reciprocity & Day 8 Pregel Routing (Q37–Q40)

#### Q37: How did we mathematically prove that our directed graph is strongly connected?
**Answer:** GraphX's `ConnectedComponents.run` proved that the graph has exactly **1 weakly connected component**. Furthermore, our directional audit verified that the graph has **100% reciprocity** (every directed edge has a matching reverse edge). By the Symmetrical Reciprocity Theorem, when a directed graph is symmetric, weak connectivity is mathematically identical to strong connectivity. Therefore, the network forms a single strongly connected component ($k=1$).

#### Q38: How will Day 8 connect Weekend 2's ML predictions to Day 7's graph?
**Answer:** In Day 8, each road segment will inherit the predicted speed $v_e$ of its geographical zone from Weekend 2's feature table. The edge weight will then be dynamically updated to travel time:
$$t_e = \frac{d_e}{\max(v_e, 5.0)}$$

#### Q39: Why does Day 7 NOT yet demonstrate dynamic rerouting?
**Answer:** Day 7 is strictly the graph construction and topology verification gate. Deriving dynamic congestion weights and running shortest-path routing algorithms is the explicit scope of Day 8. Maintaining strict milestone boundaries prevents architectural leakage and ensures rigorous auditability.

#### Q40: What algorithm will Day 8 use to compute shortest travel-time routes?
**Answer:** Day 8 will execute GraphX's native `ShortestPaths` or a custom `Pregel` program where edge weights represent dynamic travel times, computing the optimal path from an origin intersection to a destination.

---

## 🏆 14. Final Acceptance Gate Matrix & Closure Decision

| # | Acceptance Gate | Requirement | Observed Evidence | Gate Status |
| :---: | :--- | :--- | :--- | :---: |
| **1** | **Environment Compatibility** | JDK, Scala, sbt, and Spark versions mutually compatible. | Oracle JDK 1.8.0_202, Scala 2.12.18, sbt 1.9.9, Spark 3.5.0 verified. | **PASS** |
| **2** | **Build Configuration** | sbt project resolves declared dependencies reproducibly. | `build.sbt` resolved Spark Core, SQL, GraphX, and ScalaTest without errors. | **PASS** |
| **3** | **Compilation** | Scala GraphX source code compiles without errors. | `sbt compile` completed in 8.00s with 0 errors. | **PASS** |
| **4** | **Real GraphX Construction** | Creates genuine `Graph[VD, ED]` object. | `RoadGraphBuilder.buildGraph()` constructs `Graph[String, Double]`. | **PASS** |
| **5** | **Source-Grounded Input** | Identified real/synthetic datasets and schemas. | `data/intersections.csv` (20 rows) and `data/roads.csv` (40 rows) authored and whitelisted in `.gitignore`. | **PASS** |
| **6** | **Vertex Integrity** | Unique, traceable, deterministic IDs. | IDs 1..20 strictly mapped, 0 duplicates, 100% deterministic across runs. | **PASS** |
| **7** | **Edge Integrity** | Directionality, endpoints, and attributes validated. | 40 directed edges, 0 dangling endpoints, all distances finite and positive. | **PASS** |
| **8** | **Count Reconciliation** | Source rows reconcile with graph counts. | 20 source intersections = 20 vertices; 40 source roads = 40 edges. | **PASS** |
| **9** | **Automated Tests** | Comprehensive test suite passes against production code. | 13/13 tests passed in `RoadGraphBuilderSpec.scala` in 22.11s. | **PASS** |
| **10**| **Reproducibility** | Repeated execution produces consistent graph identities. | Two consecutive runs of `sbt run` yielded identical 20 vertices, 40 edges, and top degrees. | **PASS** |
| **11**| **Regression Safety** | Days 1–6 intact; models and split unchanged. | 63/63 historical tests passed; `verify_models.py` and `verify_split.py` passed 100%. | **PASS** |
| **12**| **Documentation** | Complete architecture, build, and integration docs. | Created `services/graphx-reroute/README.md`, `_day-notes/day-07/README.md`, and master compendium. | **PASS** |
| **13**| **Git Hygiene** | Scoped commits, clean working tree, no stray artifacts. | `sbt` target directories excluded; clean Conventional Commits. | **PASS** |
| **14**| **Delivery Report** | Exact commands, counts, and final verdict recorded. | Complete evidence-rich report delivered. | **PASS** |

---

## Final Milestone Closure Decision

$$\mathbf{PASS\ —\ DAY\ 7\ COMPLETE}$$

**All 14 mandatory acceptance criteria are empirically satisfied.** The road-network graph foundation is mathematically verified, topologically sound, fully tested across 13 production tests, and ready for Day 8 congestion-weighted shortest-path routing.
