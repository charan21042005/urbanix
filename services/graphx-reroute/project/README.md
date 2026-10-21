# ⚙️ sbt Build Configuration Metadata (`services/graphx-reroute/project/`)

![sbt Version](https://img.shields.io/badge/sbt-1.9.9-blue)
![Purpose](https://img.shields.io/badge/Purpose-Deterministic_Build_Pinning-brightgreen)

This directory houses the meta-build definitions for the `urbanix-graphx-reroute` Scala module.

---

## 📄 File Catalog

- **`build.properties`**: Pins the exact Scala Build Tool version:
  ```properties
  sbt.version=1.9.9
  ```
  Pinning `sbt.version` guarantees that whether the project is built on Windows 11 via `sbt.bat`, on Linux via CI/CD runners, or inside Docker containers, the exact same launcher, incremental compiler (Zinc), and resolution mechanics are employed.

---

## 🔒 Reproducibility Note
Never remove or alter `build.properties` without verifying binary compatibility with the configured Scala compiler (`2.12.18`) and Apache Spark GraphX (`3.5.0`).
