package it.unibo.big.comparison

import org.junit.Test

class JaccardComparatorTest {

  @Test
  def testJaccardSameSet(): Unit = {
    val baseSet = Set("Dog", "Doge", "Doggos")
    assert(JaccardComparator.computeJaccardIndex(baseSet, baseSet) == 1)
  }

  @Test
  def testJaccardTotallyDifferentSets():Unit = {
    val baseSet = Set("Dog", "Doge", "Doggos")
    val otherSet = Set("Cat", "Catto", "Cattos")
    assert(JaccardComparator.computeJaccardIndex(baseSet, otherSet) == 0)
  }

  @Test
  def testJaccardMixedDifferentSets():Unit = {
    val baseSet = Set("Cat", "Dog", "Doge", "Doggos")
    val otherSet = Set("Cat", "Catto", "Cattos")
    assert(JaccardComparator.computeJaccardIndex(baseSet, otherSet) == (1f/6f))
  }

  @Test
  def testJaccardAnotherMixedDifferentSets():Unit = {
    val baseSet = Set("Cat", "Dog", "Doge", "Doggos")
    val otherSet = Set("Cat", "Catto", "Cattos", "Doge", "Doggos")
    assert(JaccardComparator.computeJaccardIndex(baseSet, otherSet) == (3f/6f))
  }

  @Test
  def testItemsetOnlyArraySameItemsetTest():Unit = {
    val baseitemsetArray = Array(Set("dog", "doggos", "doggies"), Set("cat", "tiger"))
    assert(JaccardComparator.computeJaccardOnlyOnItemsetElements(baseitemsetArray, baseitemsetArray) == 1)
  }

  @Test
  def testItemsetOnlyArrayNoCommonItemsetTest():Unit = {
    val baseitemsetArray = Array(Set("dog", "doggos", "doggies"), Set("cat", "tiger"))
    val otheritemsetArray = Array(Set("mad", "man", "mann"), Set("cattos", "tigeroso"))
    assert(JaccardComparator.computeJaccardOnlyOnItemsetElements(baseitemsetArray, otheritemsetArray) == 0)
  }

  @Test
  def testItemsetOnlyArrayNoCommonItemsetSimilarValuesTest():Unit = {
    val baseitemsetArray = Array(Set("dog", "doggos", "doggies"), Set("cat", "tiger"))
    val otheritemsetArray = Array(Set("dog", "doggos"), Set("cat", "tigeroso"))
    assert(JaccardComparator.computeJaccardOnlyOnItemsetElements(baseitemsetArray, otheritemsetArray) == 0)
  }

  @Test
  def testItemsetOnlyArraySomeCommonItemsetSimilarValuesTest():Unit = {
    val baseitemsetArray = Array(Set("dog", "doggos", "doggies"), Set("cat", "tiger"))
    val otheritemsetArray = Array(Set("dog", "doggos", "doggies"), Set("cat", "tigeroso"))
    assert(JaccardComparator.computeJaccardOnlyOnItemsetElements(baseitemsetArray, otheritemsetArray) == (1f/3f))
  }

}
