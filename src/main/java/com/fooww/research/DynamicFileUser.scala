package com.fooww.research

import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object DynamicFileUser {

  def getUserSimilarityThreshold(userSimilarityRDD: RDD[(Long, Long, Double)]): RDD[(Long, Double)] = {
    val userAvgSimilarityRDD = userSimilarityRDD.map(f => {
      (f._1, f._3)
    }).groupByKey().map(f => {
      var count: Int = 0
      var sum: Double = 0
      f._2.foreach(similarity => {
        count = count + 1
        sum = sum + similarity
      })
      (f._1, sum / count)
    })
    userAvgSimilarityRDD
  }

  def getGtThresholdUsers(userSimilarityThresholdRDD: RDD[(Long, Double)], userSimilarityRDD: RDD[(Long, Long, Double)]): RDD[(Long, Long,
    Double)] = {
    val user_userSimilarity = userSimilarityRDD.map(f => {
      (f._1, (f._2, f._3))
    })
    val user_userSimilarity_thresholdRDD = user_userSimilarity.join(userSimilarityThresholdRDD)
    val user_user_similarityRDD = user_userSimilarity_thresholdRDD.filter(f => {
      f._2._1._2 > f._2._2
    })
    val resultRDD = user_user_similarityRDD.map(f => {
      (f._1, f._2._1._1, f._2._1._2)
    })
    resultRDD
  }
}
