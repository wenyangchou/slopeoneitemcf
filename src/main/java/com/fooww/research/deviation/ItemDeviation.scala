package com.fooww.research.deviation

import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object ItemDeviation {

  /**
    * 获得物品偏差矩阵,步骤4
    * @param rant user,item,rant
    * @return item,item,deviation
    */
  def getItemDeviation(rant:RDD[(Long,Long,Int)]):RDD[(Long,Long,Double)]={
    val user_item_score = rant.map(f=>{
      (f._1,(f._2,f._3))
    })
    val user_itemScore_itemScore = user_item_score.join(user_item_score)
    val itemItem_score = user_itemScore_itemScore.map(f=>{
      ((f._2._1._1,f._2._2._1),f._2._1._2 - f._2._2._2)
    })
    val itemItem_deviation = itemItem_score.reduceByKey(_+_)
    val resultRDD = itemItem_deviation.map(f=>{
      (f._1._1,f._1._2,f._2)
    })
    resultRDD
  }
}
