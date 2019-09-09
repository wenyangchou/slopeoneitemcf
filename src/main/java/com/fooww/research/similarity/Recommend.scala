package com.fooww.research.similarity

import org.apache.spark.rdd.RDD


/**
  * @author ：zwy
  */
object Recommend {

  def recommend(rantRDD:RDD[(Long,Long,Int)],devRDD:RDD[(Long,Long,Double)],itemSimilarityRDD:RDD[(Long,Long,Double)],
                userAvgSimilarity:RDD[(Long,Double)]): RDD[(Long,Long,Double)] ={
    val item_user_rant = rantRDD.map(f=>{
      (f._2,(f._1,f._3))
    })
    val item_dev = devRDD.map(f=>{
      (f._1,(f._2,f._3))
    })
    val resultRDD = item_user_rant.join(item_dev).map(f=>{
      ((f._2._1._1,f._1),f._2._1._2+f._2._2._2)
    }).reduceByKey(_+_).map(f=>{
      (f._1._1,f._1._2,f._2)
    })
    resultRDD
  }

}
