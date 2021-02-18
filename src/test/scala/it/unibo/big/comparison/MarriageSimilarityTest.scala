package it.unibo.big.comparison

import org.junit.Test

class MarriageSimilarityTest {

  @Test
  def testSameSetOfElemets(): Unit = {
    val testArray = Array(Set("feature1", "feature2"))
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray, testArray)
    assert(result._1.head == (0,0) && result._2 == 1f)
  }

  @Test
  def testDifferentSetSimilarity(): Unit = {
    val testArray = Array(Set("feature1", "feature2"))
    val testArray2 = Array(Set("feature1", "feature3"))
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray)
    assert(result._1.head == (0,0) && result._2 == (1f/3f))
  }

  @Test
  def testDifferentSetNoResultSimilarity(): Unit = {
    val testArray = Array(Set("feature1", "feature2"))
    val testArray2 = Array(Set("feature4", "feature3"))
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray)
    assert(result._1.head == (0,0) && result._2 == 0)
  }

  @Test
  def testDollAdd():Unit = {
    val testArray = Array(Set("feature1", "feature2"))
    val testArray2:Array[Set[String]] = Array(Set())
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray)
    assert(result._1.head == (0,0) && result._2 == 0)
  }

  @Test
  def testDifferentAssignementsMenWomenSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(joker, chad)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray, testArray2)
    val assigmentSet = Set((0L,1L), (1L,0L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/2f))
  }

  @Test
  def testDifferentAssignementsWomenMenSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(joker, chad)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray)
    val assigmentSet = Set((1L,0L), (0L,1L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/2f))
  }


  @Test
  def testStraightAssignementsWomanManSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(chad, joker)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray)
    val assigmentSet = Set((0L,0L), (1L,1L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/2f))
  }

  @Test
  def testStraightAssignementsManWomanSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(chad, joker)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray, testArray2)
    val assigmentSet = Set((0L,0L), (1L,1L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/2f))
  }

  @Test
  def testfilteredStraightAssignementsManWomanSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(chad, joker)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray, testArray2, true)
    val assigmentSet = Set((0L,0L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/1f))
  }

  @Test
  def testFilterdDifferentAssignementsWomenMenSimilarity(): Unit = {
    val joker = Set("gamer", "alone")
    val chad = Set("nicelooking", "alone")
    val veronica = Set("nicelooking", "alone")
    val testArray = Array(joker, chad)
    val testArray2 = Array(veronica)
    val result = MarriageSimilarity.computeMarriageSimilarity(testArray2, testArray, true)
    val assigmentSet = Set((0L,1L))
    assert(result._1.forall(assigmentSet.contains) && result._2 == (1f/1f))
  }

}
