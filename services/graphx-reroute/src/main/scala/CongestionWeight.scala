// ==============================================================================
// 🌟 URBANIX — GRAPHX CONGESTION-WEIGHTED SHORTEST-PATH ROUTING ENGINE
// CongestionWeight.scala — Travel-Time Edge Cost Modeling & Defensive Clamping
// ==============================================================================
// Welcome to the CongestionWeight module! 👋
//
// ------------------------------------------------------------------------------
// 🎓 WHAT DOES THIS SCALA FILE DO?
// ------------------------------------------------------------------------------
// In navigation applications (like Google Maps or Apple Maps), roads should NOT
// simply be weighted by physical distance (kilometers).
//
// WHY DISTANCE-BASED ROUTING FAILS IN A SMART CITY:
// Imagine two routes:
//   Route A: A short 3 km road crawling in bumper-to-bumper traffic at 10 km/h.
//            Travel Time = 3 km / 10 km/h = 0.30 hours (18 minutes!).
//   Route B: A longer 5 km highway moving freely at 45 km/h.
//            Travel Time = 5 km / 45 km/h = 0.11 hours (under 7 minutes!).
//
// If our graph engine only looked at distance, it would send ambulances and commuters
// straight into the 3 km bottleneck!
// Therefore, Urbanix optimizes for TRAVEL TIME:
//
//               base_distance_km
//   travel_time = -----------------
//                    speed_kmph
//
// ------------------------------------------------------------------------------
// 🛡️ DIVISION-BY-ZERO & THE GRIDLOCK CLAMP (1.0 km/h):
// ------------------------------------------------------------------------------
// What happens if a traffic sensor reports a speed of 0.0 km/h (complete gridlock)?
// In mathematics, dividing by zero produces Infinity:
//   3 km / 0.0 km/h = PositiveInfinity (or NaN)!
// Infinity would crash the distributed GraphX Pregel algorithm downstream.
// To prevent crashes, this file clamps any velocity below 1.0 km/h to exactly 1.0 km/h.
// A 3 km road at 1.0 km/h takes 3.0 hours—heavily penalizing the road without breaking math!
// ==============================================================================

package urbanix.graphx

// 'package' groups related Scala classes together into a logical namespace.
// In Scala, 'object' declares a SINGLETON—a unique object with static-like functions.
// You don't need to type 'new CongestionWeight()'; you can call 'CongestionWeight.calculateTravelTimeHours(...)' directly!
object CongestionWeight {

  /**
   * Default minimum speed floor in km/h used to prevent division by zero during gridlock.
   * 'val' declares an immutable (read-only) constant.
   */
  val DefaultMinSpeedFloorKmph: Double = 1.0

  /**
   * Calculates estimated travel time in hours for a directed road segment.
   *
   * @param baseDistanceKm    Physical road distance in kilometers (must be > 0.0).
   * @param speedKmph         Current or predicted vehicle velocity in km/h.
   * @param minSpeedFloorKmph Minimum speed clamp (default: 1.0 km/h).
   * @return                  Estimated travel time in hours.
   */
  def calculateTravelTimeHours(
    baseDistanceKm: Double,
    speedKmph: Double,
    minSpeedFloorKmph: Double = DefaultMinSpeedFloorKmph
  ): Double = {

    // -------------------------------------------------------------------------
    // 🔍 STEP 1: DEFENSIVE INPUT VALIDATION
    // -------------------------------------------------------------------------
    // 'isNaN' checks for "Not a Number". 'isInfinity' checks for overflow.
    // In production systems, we reject corrupted inputs before doing math!
    if (baseDistanceKm.isNaN || baseDistanceKm.isInfinity) {
      throw new IllegalArgumentException(s"Base distance cannot be NaN or Infinite: $baseDistanceKm")
    }
    // A road cannot have zero or negative length in the real world
    if (baseDistanceKm <= 0.0) {
      throw new IllegalArgumentException(s"Base distance must be strictly positive (> 0.0 km), observed: $baseDistanceKm")
    }

    // Validate the minimum speed floor parameter
    if (minSpeedFloorKmph.isNaN || minSpeedFloorKmph.isInfinity || minSpeedFloorKmph <= 0.0) {
      throw new IllegalArgumentException(s"Minimum speed floor must be strictly positive and finite, observed: $minSpeedFloorKmph")
    }

    // Validate speed parameter
    if (speedKmph.isNaN || speedKmph.isInfinity) {
      throw new IllegalArgumentException(s"Vehicle speed cannot be NaN or Infinite: $speedKmph")
    }
    // Vehicles cannot travel at negative speed in our routing model
    if (speedKmph < 0.0) {
      throw new IllegalArgumentException(s"Vehicle speed cannot be negative (< 0.0 km/h), observed: $speedKmph")
    }

    // -------------------------------------------------------------------------
    // 🛡️ STEP 2: APPLY GRIDLOCK FLOOR CLAMPING
    // -------------------------------------------------------------------------
    // In Scala, 'if / else' is an expression that yields a value directly!
    val effectiveSpeedKmph = if (speedKmph < minSpeedFloorKmph) {
      minSpeedFloorKmph // Clamp 0.0 km/h to 1.0 km/h
    } else {
      speedKmph         // Keep normal velocity
    }

    // -------------------------------------------------------------------------
    // ⏱️ STEP 3: CALCULATE TRAVEL TIME IN HOURS
    // -------------------------------------------------------------------------
    val travelTimeHours = baseDistanceKm / effectiveSpeedKmph

    // Final safety check: Result must be a valid positive number
    if (travelTimeHours.isNaN || travelTimeHours.isInfinity || travelTimeHours <= 0.0) {
      throw new IllegalStateException(s"Calculated travel time is invalid ($travelTimeHours) for dist=$baseDistanceKm, speed=$effectiveSpeedKmph")
    }

    // In Scala, the last expression evaluated in a function is automatically returned!
    // (You don't even have to type 'return travelTimeHours')
    travelTimeHours
  }

  /**
   * Helper function: Converts hours to minutes for operator dashboards and logging.
   * Example: 0.25 hours * 60 = 15.0 minutes.
   */
  def hoursToMinutes(hours: Double): Double = hours * 60.0
}
