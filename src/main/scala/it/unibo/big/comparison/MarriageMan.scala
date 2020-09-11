package it.unibo.big.comparison

/**
 * Trait for defining a potential married man and the operation computed inside the marriage problem.
 */
trait MarriageMan {

  /**
   * The id of the Marriage Man.
   * @return a Long value
   */
  def id:Long

  /**
   * The feature defining the Man and its requirements.
   * @return a set containing strings.
   */
  def featureSet:Set[String]

  /**
   * Define if the current man has got already a partner.
   * @return true if the man is taken, false otherwise.
   */
  def isSingle:Boolean

  /**
   * Set the current man as engaged.
   * @param marriageMan the potential partner to be associated.
   */
  def setEngaged(marriageMan: MarriageMan):Unit

  /**
   * Break up the relation between the partner and this man.
   */
  def setSingle():Unit

  /**
   * Compute the similarity between a potential partner and this man
   * @param potentialPartner the potential partner to be matched.
   * @return a float containing the similarity match.
   */
  def computePartnerSimilarity(potentialPartner:MarriageMan):Float

  /**
   * Return the current partner, if present.
   * @return an optional containing the current partner.
   */
  def getCurrentPartner():Option[MarriageMan]

  /**
   * Getter for the current similarity
   * @return a float containing the current similarity.
   */
  def getCurrentSimilarity():Float

}

object MarriageMan {

  type MarriageWoman = MarriageMan

  def apply(id:Long, featureSet:Set[String]): MarriageMan = new MarriageManImpl(id, featureSet)

  def apply(id:Long):MarriageMan = new MarriageManImpl(id, Set())

  private class MarriageManImpl(val id:Long, val featureSet:Set[String]) extends MarriageMan {

    var partner:Option[MarriageMan] = None
    var currentSimilarity:Float = 0f

    override def isSingle: Boolean = partner.isEmpty

    override def setEngaged(marriageMan: MarriageMan): Unit = {
      partner = Some(marriageMan)
      currentSimilarity = computePartnerSimilarity(marriageMan)
    }

    override def setSingle(): Unit = {
      partner = None
      currentSimilarity = 0
    }

    override def computePartnerSimilarity(potentialPartner: MarriageMan): Float =
      JaccardComparator.computeJaccardIndex(featureSet, potentialPartner.featureSet)

    override def getCurrentPartner(): Option[MarriageMan] = partner

    override def getCurrentSimilarity(): Float = currentSimilarity
}
}
