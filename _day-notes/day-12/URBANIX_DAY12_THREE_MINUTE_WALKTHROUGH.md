# ⏱️ URBANIX — THREE-MINUTE PROJECT WALKTHROUGH & PRESENTATION GUIDE
## Academic Capstone Defense Script, Live Screen Actions, Examiner Interruption Handling & Contingency Fallback

**Project:** Urbanix — Smart City IoT Traffic & Environmental Risk Forecasting Platform  
**Target Delivery Window:** 3 Minutes (180 Seconds) Strict Cap  
**Tone:** Confident, Authoritative, Evidence-Based, Beginner-Accessible  

---

## 1. Master Spoken Script (Word-for-Word, Timed)

### 0:00 – 0:30 (30 Seconds) — Problem Statement & The "Why"
> "Good morning, evaluators.  
> Most smart city traffic platforms today are entirely **reactive**: they detect congestion only after vehicles are already stuck in gridlock. By the time emergency vehicles or commuter apps reroute, the bottleneck has already paralyzed the corridor.  
>
> With **Urbanix**, we built a distributed cluster analytics platform that shifts the paradigm from *reactive reporting* to **predictive forecasting**. We ingest real-time traffic and environmental sensor streams, aggregate them across 5-minute spatial tumbling windows, forecast highway speeds before queues form, and dynamically reroute vehicles through a genuine Scala Spark GraphX engine."

### 0:30 – 1:15 (45 Seconds) — Distributed Architecture in One Breath
> "Here is our end-to-end distributed data architecture:  
> 1. At the ingestion edge, three independent Python producers replay historical telemetry from Caltrans PeMS and EPA AirData into separate Apache Kafka topics, converting velocities to kilometers per hour.  
> 2. Apache Spark Structured Streaming consumes these streams, enforces a 2-minute event-time watermark to bound memory, and aggregates data into 5-minute tumbling windows enriched with spatial grid zones.  
> 3. These windowed aggregates are committed to an append-only columnar Apache Parquet feature store.  
> 4. Our Spark MLlib regression models score the latest features to forecast velocity for the subsequent 5-minute interval.  
> 5. Next, a dedicated Scala Spark GraphX engine reads those predictions, re-weights 40 directed road edges using travel-time costs, and computes Single-Source Shortest Paths using Google's Bulk-Synchronous Parallel Pregel algorithm.  
> 6. Finally, an asynchronous FastAPI backend scans these Parquet snapshots fresh per request, serving an auto-refreshing geospatial Leaflet dashboard."

### 1:15 – 2:15 (60 Seconds) — Live Demonstration & Model Proof
> *(Action: Point to the browser dashboard on screen)*  
> "Looking at the live dashboard:  
> Here you see our 5 municipal zones and 20 physical road intersections across the Los Angeles metropolitan grid.  
> In the top KPI ribbon, the platform displays the current network state. For municipal Zone 1, our latest predicted velocity is **77.7 km/h**, derived from our trained MLlib Linear Regression model.  
>
> On our held-out, strictly chronological test split, this model achieved a verified **$R^2$ of 0.7757** with an **RMSE of 2.93 km/h**, successfully meeting our roadmap milestone gate of 0.75.  
>
> Down below, for Destination 15 — Long Beach Boulevard & PCH — the GraphX Pregel engine has recommended the optimal corridor: `1 -> 2 -> 14 -> 16 -> 15`, with a total calculated travel time of **19.02 minutes**.  
> Notice the amber badge: our dashboard actively evaluates source data timestamps against operational freshness thresholds. Because this is replayed historical benchmark data, the system honestly labels it as **STALE**, proving that our freshness detector derives from true data timestamps rather than client-side browser polling."

### 2:15 – 2:45 (30 Seconds) — Technical Differentiator (Scala GraphX & Reliability)
> "What sets Urbanix apart from typical capstone projects is our technical differentiator:  
> First, our routing engine is built in **native Scala Spark GraphX**, not a Python NetworkX substitute. We rejected GraphX's built-in `ShortestPaths` library because it only computes unweighted hop counts. Instead, we implemented custom Pregel message passing where edge weights represent true physical travel time: $\text{distance} / \text{predicted\_speed}$.  
>
> Second, our system is engineered for graceful degradation. In our fault-tolerance tests, when feature tables are missing, the API returns a structured HTTP 503 instead of crashing, and the dashboard frontend retains the last-known route with clear visual warnings."

### 2:45 – 3:00 (15 Seconds) — Conclusion & Future Work
> "In summary, Urbanix proves that distributed streaming, machine-learned forecasting, and graph-parallel algorithms can close the loop from sensor ingestion to real-time navigation.  
> As a next step, we would connect physical hardware MQTT sensors and expand the MLlib pipeline to forecast air quality alongside traffic.  
> Thank you, and I welcome your questions."

---

## 2. Step-by-Step Live Demo Action Checklist

| Timestamp | Spoken Point | Action on Screen / Terminal | Expected Visual Observation |
| :---: | :--- | :--- | :--- |
| **0:00** | Introduction & Problem | Open browser to Dashboard (`http://localhost:8085` or `http://localhost:8000`). | Dark glassmorphic dashboard loads; title visible: *Urbanix Smart City Dashboard*. |
| **0:45** | Architectural Data Flow | Hover mouse over the KPI Ribbon (Predictions, Routes, Cadence). | Visual indicators: Predictions: 5 Active Zones; Average Speed; Route ETA. |
| **1:15** | Live Predictions & Freshness | Point to Zone 1 card (`77.67 km/h`) and amber `STALE` badge. | Amber badge: *Predictions: Stale (2012 Replay)*. Explains timestamp evaluation. |
| **1:40** | Recommended Route & Map | Point to Map center and the blue PolyLine connecting nodes. | Route PolyLine connects Civic Center (Node 1) to Long Beach (Node 15). Destination 15 card shows ETA `19.0m`. |
| **2:15** | GraphX & MLlib Differentiator | Open terminal tab or show model README snippet (`R² = 0.7757`). | Terminal shows verified metrics table or test output: 47 passed dashboard tests, 9 passed model tests. |
| **2:45** | Conclusion & Q&A | Return to dashboard full screen; invite questions. | Clean, stable dashboard displaying map and zone metrics. |

---

## 3. Contingency Fallback Plan (If Live Demo Fails)

If a container crashes, a port conflict blocks startup, or network connectivity fails during your live evaluation, execute this **Zero-Panic Fallback Protocol**:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        LIVE DEMO CONTINGENCY PROTOCOL                       │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. DO NOT PANIC OR FRANTICALLY TYPE COMMANDS.                               │
│ 2. CALMLY STATE THE ARCHITECTURAL DEFENSE:                                  │
│    "Evaluators, as we documented in Day 11's Reliability and Graceful        │
│    Degradation phase, distributed systems must expect infrastructure       │
│    failures. Let me show you how Urbanix handles this."                     │
│ 3. SWITCH TO THE CACHED SCREENSHOT / TEST REPORT:                           │
│    Open `dashboard_full.png` or `FULL_SYSTEM_TEST.md`.                      │
│ 4. RUN THE OFFLINE AUTOMATED SUITE (ZERO RUNTIME DEPENDENCIES):              │
│    .\dashboard\venv\Scripts\pytest -v dashboard/tests/                      │
│    Shows 47 passing tests verifying API contracts, PolyLine rendering, and  │
│    error degradation without needing active network ports!                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Likely Examiner Interruptions & Rapid Responses

### Interruption 1: "Why does your dashboard say 'STALE'?"
> **Rapid Answer:**  
> "Because our dashboard derives freshness from the **actual telemetry timestamp**, not the time when the browser refreshed. The historical Caltrans PeMS benchmark dataset is from March 2012. Our freshness evaluator enforces a 15-minute threshold on predictions. Because the data is from 2012, it honestly flags it as STALE. If we falsified the timestamps to match today's clock, it would say FRESH, but that would be epistemically dishonest."

### Interruption 2: "Did you use NetworkX or GraphFrames in Python for routing?"
> **Rapid Answer:**  
> "No, sir. We deliberately built a genuine **Scala Spark GraphX** module compiled with sbt against Apache Spark 3.5.1. GraphX runs natively inside the JVM using Google's Bulk-Synchronous Parallel Pregel algorithm. We share data between Python and Scala through an append-only columnar Parquet contract on disk, avoiding brittle Py4J socket connections."

### Interruption 3: "Why did you use Linear Regression instead of Deep Learning (LSTM)?"
> **Rapid Answer:**  
> "For a 5-minute tumbling window with a streaming SLA, Linear Regression in Spark MLlib converges in milliseconds via Quasi-Newton optimization and achieved an $R^2$ of 0.7757 with an RMSE of 2.93 km/h. An LSTM or neural network requires extensive GPU infrastructure and introduces inference latency that can violate streaming SLAs without proportional accuracy gains on 5-minute aggregations."

### Interruption 4: "Why did Decision Tree perform poorly ($R^2 < 0$)?"
> **Rapid Answer:**  
> "Our Decision Tree regressor with depth 8 produced 307 leaf nodes across only 365 training samples, causing severe overfitting. When evaluated on our out-of-sample morning rush-hour test split, its variance was higher than the mean baseline. Disclosing this negative result proves we tested on an unseen chronological split rather than overfitting or fabricating results."

---

## 5. Rehearsal Tracking Plan
- [ ] **Rehearsal 1 (Closed-book, timed):** Recorded time: _____ seconds. (Focus: Pacing and eliminating filler words).
- [ ] **Rehearsal 2 (With screen actions):** Recorded time: _____ seconds. (Focus: Synchronizing speech with mouse clicks on dashboard).
- [ ] **Rehearsal 3 (Interruption defense):** Recorded time: _____ seconds. (Focus: Rapid, confident delivery of examiner responses).
