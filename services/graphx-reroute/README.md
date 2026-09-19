# 🕸️ Congestion-Aware Rerouting (GraphX)
![Status](https://img.shields.io/badge/Status-Planned%20(Days%207--9)-blueviolet)

## Purpose
Calculates optimal routes across the city network by dynamically adjusting road traversal weights based on ML-predicted congestion and environmental risks.

## Graph Concept
- **Vertices:** City intersections.
- **Edges:** Road segments connecting intersections.
- **Congestion Weight:** Dynamic edge weights updated via Spark Streaming predictions.
- **Shortest Path:** Graph traversal to find the lowest-cost route.

*Note: This module is intentionally implemented in Scala to leverage native JVM Apache Spark GraphX capabilities.*

**Current Status:** Planned for Days 7–9. No Scala/GraphX code implemented yet.
