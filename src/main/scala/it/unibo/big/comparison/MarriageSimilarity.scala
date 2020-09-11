package it.unibo.big.comparison

import it.unibo.big.comparison.MarriageMan.MarriageWoman

import scala.util.control.Breaks._

/**
 * Class to compute Marriage Similarity between two sets.
 */
object MarriageSimilarity {


  private def convertArrayToMarriageSet(baseSet:Array[Set[String]]):Array[MarriageMan] =
    baseSet
      .zipWithIndex
    .map(tuple => MarriageMan(tuple._2, tuple._1))


  private def redPill(joker:MarriageMan, chad:MarriageMan, veronica:MarriageWoman):Unit = {
    joker.setSingle()
    engageCouple(chad, veronica)
    println(s" Joker ${joker.id} said: Gamer rise up")
  }

  private def engageCouple(chad:MarriageMan, veronica:MarriageWoman):Unit = {
    chad.setEngaged(veronica)
    veronica.setEngaged(chad)
  }

  private def addDolls(marriageArray:Array[MarriageMan], threshold:Int): Array[MarriageMan] = {
    var newMarriageArray = marriageArray
    while (newMarriageArray.length != threshold) {
      newMarriageArray = newMarriageArray :+ MarriageMan.apply(marriageArray.length)
    }
    newMarriageArray.foreach(l => println(s"${l.id} : ${l.featureSet.size}"))
    newMarriageArray
  }


  def computeMarriageSimilarity(maleSet:Array[Set[String]],
                                femaleSet:Array[Set[String]], removeUnmatched:Boolean = false):(Set[(Long, Long)], Float) = {
    if(maleSet.length == 0 || femaleSet.length == 0) {
      (Set(), 0)
    } else {
      var marriageManArray = convertArrayToMarriageSet(maleSet)
      var marriageWomanArray = convertArrayToMarriageSet(femaleSet)
      val max_number = scala.math.max(marriageManArray.length, marriageWomanArray.length)
      if(marriageManArray.length == max_number) {
        marriageWomanArray = addDolls(marriageWomanArray, max_number)
      } else {
        marriageManArray = addDolls(marriageManArray, max_number)
      }
      var freeMan = max_number
      while(freeMan > 0) {
        val currentMan = marriageManArray.filter(_.isSingle).head
        val sortedWomanArray = marriageWomanArray.sortBy( elem => -1 * currentMan.computePartnerSimilarity(elem))
        breakable {
          for(i <- 0 to sortedWomanArray.length) {
            sortedWomanArray(i) match {
              case currentWoman:MarriageWoman if currentWoman.isSingle => {
                engageCouple(currentWoman, currentMan)
                freeMan = freeMan-1
                break()
              }
              case currentWoman:MarriageWoman => {
                if(currentMan.computePartnerSimilarity(currentWoman) > currentWoman.getCurrentSimilarity()){
                  redPill(currentWoman.getCurrentPartner().get, currentMan, currentWoman)
                  break()
                }
              }
            }
          }
        }
      }
      val definitiveManSet = {if(removeUnmatched) {
        marriageManArray.filter(man => man.getCurrentPartner().get.featureSet.nonEmpty &&
          man.featureSet.nonEmpty)
      } else {
        marriageManArray
      }}

      val currentSetSimilarity = definitiveManSet
        .map(_.getCurrentSimilarity())
        .sum / definitiveManSet.length.toFloat
      (definitiveManSet.map(man => (man.id, man.getCurrentPartner().get.id)).toSet, currentSetSimilarity)
    }

  }
}
