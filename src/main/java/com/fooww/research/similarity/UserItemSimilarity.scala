package com.fooww.research.similarity

import org.apache.spark.rdd.RDD

object UserItemSimilarity {

  /**
    * 计算用户评分之间的均差，步骤3
    * @param rant user,item,score
    * @param userSimilarity user,user,similarity
    * @return user,item,averageSimilarity
    */
  def getUserItemAverageSimilarity(rant:RDD[(Long,Long,Int)],userSimilarity: RDD[(Long,Long,Double)]):RDD[(Long,Long,Double)]={

    val item_number = rant.map(f=>(f._2,1)).reduceByKey(_+_)

    val item_user = rant.map(f=>(f._2,f._1))
    val userUser_item = item_user.join(item_user).map(f=>(f._2,f._1))
    val userUser_similarity = userSimilarity.map(f=>((f._1,f._2),f._3))

    val item_user_similarity = userUser_item.join(userUser_similarity).map(f=>(f._2._1,(f._1._1,f._2._2)))
    val item_user_similarity_number = item_user_similarity.join(item_number)
    val userItem_usersimilarity = item_user_similarity_number.map(f=>{
      if (f._2._2==1){
        ((f._2._1._1,f._1),1)
      }else{
        ((f._2._1._1,f._1),f._2._1._2/(f._2._2-1))
      }
    })

    val userItem_averageSimilarity = userItem_usersimilarity.reduceByKey(_+_)
    val result = userItem_averageSimilarity.map(f=>(f._1._1,f._1._2,f._2))
    result
  }

}
