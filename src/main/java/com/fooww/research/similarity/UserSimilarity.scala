package com.fooww.research.similarity

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object UserSimilarity {

  //余弦相似度
  def getCosineSimilarity(rantRDD:RDD[(Long,Long,Int)]):RDD[(Long,Long,Double)]={
    val item_user_score = rantRDD.map(f => (f._2, (f._1, f._3)))

    val item_user_score_user_score = item_user_score.join(item_user_score)

    val user_user_score_score = item_user_score_user_score.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    val user_user_vectorProduct = user_user_score_score.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)

    val diagonal_matrix = user_user_vectorProduct.filter(f => f._1._1 == f._1._2)
    val user_userSquare = diagonal_matrix.map(f => (f._1._1, f._2))
    val nonDiagonal_matrix = user_user_vectorProduct.filter(f => f._1._1 != f._1._2)

    val user1_user1_user2_vectorProduct_user1Square = nonDiagonal_matrix.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(user_userSquare)

    val user2_user1_user2_product_user1Square_user2Square = user1_user1_user2_vectorProduct_user1Square.map(f => (f._2._1._2, (f._1, f._2._1._2, f._2._1._3, f._2._2))).join(user_userSquare)

    val user1_user2_product_user1Square_user2Square = user2_user1_user2_product_user1Square_user2Square.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    val user1_user2_similarity = user1_user2_product_user1Square_user2Square.map(f => (f._1, f._2, (f._3 / (sqrt(f._4) * sqrt(f._5))).formatted("%.2f").toDouble))
    user1_user2_similarity
  }
}
