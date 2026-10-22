// ==============================================================================
// Urbanix — Day 8: GraphX Congestion-Weighted Shortest-Path Routing Engine
// CongestionWeight.scala — Travel-Time Edge Cost Modeling & Defensive Clamping
// ==============================================================================
// In a smart city routing engine, a road's routing cost should not merely represent
// Euclidean physical distance. Instead, routing should optimize for TRAVEL TIME:
// How long will a vehicle take to traverse this specific road corridor?
//
// Formula:
//   travel_time_hours = base_distance_km / speed_kmph
//
// Units:
//   - base_distance_km: Distance along the road in kilometers (km > 0)
//   - speed_kmph: Average or predicted vehicle speed in kilometers per hour (km/h > 0)
//   - Result: Estimated travel time in hours (hours > 0)
//
// Why Distance Alone Fails:
//   A 3 km highway segment crawling at 10 km/h takes 18 minutes (0.30 hours).
//   A 5 km arterial segment moving at 45 km/h takes 6.67 minutes (0.111 hours).
//   Minimizing distance chooses the 3 km road; minimizing travel time chooses the 5 km road!
//
// Division-by-Zero & Gridlock Floor (1.0 km/h):
//   If a sensor reports 0.0 km/h (severe traffic jam / complete gridlock), dividing
//   distance by 0 would produce Double.PositiveInfinity or NaN, crashing the downstream
//   GraphX Pregel relaxation algorithm.
//   To model extreme gridlock defensibly, speeds below `minSpeedFloorKmph` (default: 1.0 km/h)
//   are clamped to 1.0 km/h. At 1.0 km/h, a 3 km road costs 3.0 hours, accurately penalizing
//   the road without breaking numerical stability.
// ==============================================================================

package urbanix.graphx

object CongestionWeight {

  /**
   * Default minimum speed floor in km/h used to prevent division by zero during gridlock.
   */
  val DefaultMinSpeedFloorKmph: Double = 1.0

  /**
   * Calculates the estimated travel time in hours for a road segment given its base distance
   * and current or predicted vehicle velocity.
   *
   * @param baseDistanceKm Physical length of the road in kilometers (must be strictly positive and finite).
   * @param speedKmph Vehicle speed in kilometers per hour (must be finite and non-negative).
   * @param minSpeedFloorKmph Minimum speed clamp used when speed is below this threshold (default: 1.0 km/h).
   * @return Estimated travel time in hours (strictly positive and finite).
   * @throws IllegalArgumentException if distance or speed inputs are non-finite, negative, or invalid.
   */
  def calculateTravelTimeHours(
    baseDistanceKm: Double,
    speedKmph: Double,
    minSpeedFloorKmph: Double = DefaultMinSpeedFloorKmph
  ): Double = {
    // 1. Validate baseDistanceKm
    if (baseDistanceKm.isNaN || baseDistanceKm.isInfinity) {
      throw new IllegalArgumentException(s"Base distance cannot be NaN or Infinite: $baseDistanceKm")
    }
    if (baseDistanceKm <= 0.0) {
      throw new IllegalArgumentException(s"Base distance must be strictly positive (> 0.0 km), observed: $baseDistanceKm")
    }

    // 2. Validate minSpeedFloorKmph
    if (minSpeedFloorKmph.isNaN || minSpeedFloorKmph.isInfinity || minSpeedFloorKmph <= 0.0) {
      throw new IllegalArgumentException(s"Minimum speed floor must be strictly positive and finite, observed: $minSpeedFloorKmph")
    }

    // 3. Validate speedKmph
    if (speedKmph.isNaN || speedKmph.isInfinity) {
      throw new IllegalArgumentException(s"Vehicle speed cannot be NaN or Infinite: $speedKmph")
    }
    if (speedKmph < 0.0) {
      throw new IllegalArgumentException(s"Vehicle speed cannot be negative (< 0.0 km/h), observed: $speedKmph")
    }

    // 4. Apply gridlock floor clamping:
    // If speed is zero or very close to zero, clamp to minSpeedFloorKmph to avoid division by zero.
    val effectiveSpeedKmph = if (speedKmph < minSpeedFloorKmph) {
      minSpeedFloorKmph
    } else {
      speedKmph
    }

    // 5. Compute travel time in hours
    val travelTimeHours = baseDistanceKm / effectiveSpeedKmph

    // Defensive assertion: result must be strictly positive and finite
    if (travelTimeHours.isNaN || travelTimeHours.isInfinity || travelTimeHours <= 0.0) {
      throw new IllegalStateException(s"Calculated travel time is invalid ($travelTimeHours) for dist=$baseDistanceKm, speed=$effectiveSpeedKmph")
    }

    travelTimeHours
  }

  /**
   * Helper function to convert hours to minutes for human-readable logging and dashboards.
   */
  def hoursToMinutes(hours: Double): Double = hours * 60.0
}
