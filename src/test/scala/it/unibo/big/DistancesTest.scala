package it.unibo.big

import it.unibo.big.Utils._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class DistancesTest extends JUnitSuite {
  @Test def distances {
    assert(Distances.haversine(44, 9, 44, 9) == 0)
    assert(Distances.haversine(44, 9, 44, 9) == Distances.haversineEuclideanApproximation(44, 9, 44, 9))
    assert(Math.abs(Distances.haversine(44.01, 9.01, 44, 9) - Distances.haversineEuclideanApproximation(44.01, 9.01, 44, 9)) < 1)
    assert(Math.abs(Distances.haversine(44.1, 9.1, 44, 9) - Distances.haversineEuclideanApproximation(44.1, 9.1, 44, 9)) < 10)
    // TODO there can be a huge gap btw haversine and its approximation
    // assert(Distances.haversine(45.4245, 9.1575, 45.4245, 9.2025) == Distances.haversineEuclideanApproximation(45.4245, 9.1575, 45.4245, 9.2025))
  }
}
