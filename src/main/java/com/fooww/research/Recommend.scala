package com.fooww.research

import com.fooww.research.deviation.ItemDeviation
import com.fooww.research.predict.Predict
import com.fooww.research.similarity.{ItemSimilarity, UserItemSimilarity, UserSimilarity}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

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

  def getCFRDD(rant:RDD[(Long,Long,Int)]):RDD[(Long,Long,Double)]={

    val rank = 100
    val numIteration = 100
    val rantings = rant.map(f=>Rating(f._1.toInt,f._2.toInt,f._3))

    val model = ALS.train(rantings,rank,numIteration,0.01)

    val userRDD = rant.map(_._1.toInt).distinct()
    val itemRDD = rant.map(_._2.toInt).distinct()
    val userItemRDD = userRDD cartesian itemRDD
    val predictRDD = model.predict(userItemRDD).map(f=>(f.user.toLong,f.product.toLong,f.rating))
    predictRDD
  }

}
