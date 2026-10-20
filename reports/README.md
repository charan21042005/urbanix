# 📊 Urbanix Evaluation Reports & Metric Manifests (`reports/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Audited-brightgreen)
![Tracking](https://img.shields.io/badge/Provenance-Machine--Readable_JSON-blue)
![Format](https://img.shields.io/badge/Reports-Markdown_%26_JSON-orange)

This directory serves as the centralized repository for **machine-readable evaluation manifests, performance audit tables, and mathematical verification checklists** across the 12-day Urbanix roadmap.

---

## 📁 1. Directory Structure by Milestone

```
reports/
└── day-05/                             # Day 5 MLlib Model Training & Evaluation Artifacts
    ├── README.md                       # Navigation guide and artifact breakdown
    ├── metrics.json                    # Machine-readable evaluation manifest (provenance, weights, metrics)
    ├── per_zone_metrics.json           # Granular per-zone metric breakdown (zones 1 to 5)
    ├── results.md                      # Formatted Markdown report with comparison tables
    └── verification.md                 # Post-training independent audit checklist
```

---

## 🏛️ 2. Milestone Overview

| Report Subfolder | Milestone Phase | Primary Deliverables | Key Output Metrics | Status |
| :--- | :--- | :--- | :--- | :---: |
| [`day-05/`](day-05/README.md) | Weekend 2, Day 5: MLlib Model Training & Evaluation | Machine-readable manifests, per-zone breakdowns, verification audit | **Linear Regression: $R^2 = 0.7757$ (Passed > 0.75)**; Decision Tree: $R^2 = -0.2064$ | **COMPLETE ✅** |

---

## 🔒 3. Academic & Audit Standards

1. **Machine-Readable Traceability:** All numeric scores reported in academic thesis chapters and paper submissions are derived directly from `metrics.json`.
2. **Reproducibility Guarantee:** Independent verification tools in `services/streaming/` assert that reloaded model predictions match these manifests within $10^{-4}$ numerical tolerance.
3. **Immutability:** Once a milestone is verified and pushed to `origin/main`, its evaluation manifests serve as permanent baseline records.
