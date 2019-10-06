package com.fooww.research.similarity

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object ItemSimilarity {

  /**
    * 物品与物品的余弦相似度,步骤5
    * @param rantRDD  user,item,rant
    * @return item,item,similarity
    */
  def getCosineSimilarity(rantRDD:RDD[(Long,Long,Int)]):RDD[(Long,Long,Double)]={
    val user_item_score = rantRDD.map(f => (f._1, (f._2, f._3)))

    val user_item_score_item_score = user_item_score.join(user_item_score)

    val item_item_score_score = user_item_score_item_score.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    val item_item_vectorProduct = item_item_score_score.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)

    val diagonal_matrix = item_item_vectorProduct.filter(f => f._1._1 == f._1._2)
    val item_itemSquare = diagonal_matrix.map(f => (f._1._1, f._2))
    val nonDiagonal_matrix = item_item_vectorProduct.filter(f => f._1._1 != f._1._2)

    val item1_item1_item2_vectorProduct_item1Square = nonDiagonal_matrix.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(item_itemSquare)

    val item2_item1_item2_product_item1Square_item2Square = item1_item1_item2_vectorProduct_item1Square.map(f => (f._2._1._2, (f._1, f._2._1._2, f._2._1._3, f._2._2))).join(item_itemSquare)

    val item1_item2_product_item1Square_item2Square = item2_item1_item2_product_item1Square_item2Square.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    val item1_item2_similarity = item1_item2_product_item1Square_item2Square.map(f => (f._1, f._2, (f._3 / (sqrt(f._4) * sqrt(f._5))).formatted("%.2f").toDouble))
    item1_item2_similarity
  }
}
