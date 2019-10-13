package com.fooww.research

import com.fooww.research.deviation.ItemDeviation
import com.fooww.research.predict.Predict
import com.fooww.research.similarity.{ItemSimilarity, UserItemSimilarity, UserSimilarity}
import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object Recommend {

  def getSlopeRDD(rant:RDD[(Long,Long,Int)]):RDD[(Long, Long, Double)]={
    val itemSimRDD = ItemSimilarity.getCosineSimilarity(rant)
    val userItemSimRDD = UserItemSimilarity.getUserItemAverageSimilarity(rant,userItemSimRDD)
    val itemDevRDD = ItemDeviation.getItemDeviation(rant)
    Predict.getPredict(itemDevRDD,rant,itemSimRDD,userItemSimRDD)
  }

}
