# 📂 GraphX Reroute Engine Source Tree (`services/graphx-reroute/src/`)

![Language](https://img.shields.io/badge/Language-Scala_2.12.18-red)
![Layout](https://img.shields.io/badge/Architecture-Standard_sbt%2FMaven_Tree-blue)
![Engine](https://img.shields.io/badge/Engine-Apache_Spark_GraphX_3.5.0-orange)

This directory houses the Scala source code for the **Urbanix GraphX Road Network Engine**, structured according to standard sbt and Maven conventions.

---

## 🏛️ Directory Structure

```
services/graphx-reroute/src/
├── main/
│   └── scala/
│       └── RoadGraphBuilder.scala     # Production two-phase source validator, graph builder, & audit engine
└── test/
    └── scala/
        └── RoadGraphBuilderSpec.scala # 13-test automated unit and integration verification suite
```

---

## 📄 Source File Catalog

### 1. `main/scala/RoadGraphBuilder.scala`
The core production singleton object (`urbanix.graphx.RoadGraphBuilder`):
- `validateSourceData(intersectionsDF, roadsDF)`: Pre-construction data quality audit evaluating 10 distinct topological invariants.
- `buildGraph(spark, intersectionsDF, roadsDF)`: Materializes the distributed property graph as `Graph[String, Double]` from `VertexRDD[(VertexId, String)]` and `EdgeRDD[Edge[Double]]`.
- `auditGraph(graph, intersectionsDF, roadsDF)`: Post-construction audit identifying phantom vertices, dangling edges, self-loops, and component reciprocity.
- `validateAndBuildGraph(spark, intersectionsDF, roadsDF, strict = true)`: Production entry point enforcing fail-fast exception policies.
- `main(args)`: CLI application constructing the graph from `data/intersections.csv` and `data/roads.csv`.

### 2. `test/scala/RoadGraphBuilderSpec.scala`
The automated test suite extending `org.scalatest.funsuite.AnyFunSuite`:
- Implements 13 production-oriented test cases validating basic construction, deterministic ID mapping, duplicate detection, dangling edges, invalid distances, directionality, self-loops, strict exceptions, and GraphX operators (`triplets`, `inDegrees`, `outDegrees`, `subgraph`).

---

## ⚡ Execution Commands
```powershell
# From services/graphx-reroute/
.\sbt.bat --batch compile
.\sbt.bat --batch test
.\sbt.bat --batch run
```
