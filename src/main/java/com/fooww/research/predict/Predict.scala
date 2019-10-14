package com.fooww.research.predict

import org.apache.spark.rdd.RDD

object Predict {

  def getPredict(dev: RDD[(Long, Long, Double)], rant: RDD[(Long, Long, Double)], itemSim: RDD[(Long, Long, Double)],
                 userItemSim: RDD[(Long, Long, Double)]): RDD[(Long, Long, Double)] = {

    val item_user_rant = rant.map(f => (f._2, (f._1, f._3)))
    val item_item_dev = dev.map(f => (f._1, (f._2, f._3)))
    val item_item_user_rant_dev = item_user_rant.join(item_item_dev).map(f => (f._1, (f._2._2._1, f._2._1._1, f._2._1._2, f._2._2._2)))
    val itemItem_user_rant_dev = item_item_user_rant_dev.map(f => ((f._1, f._2._1), (f._2._2, f._2._3, f._2._4)))

    val itemItem_sim = itemSim.map(f => ((f._1, f._2), f._3))
    val itemItem_user_rant_dev_itemsim = itemItem_user_rant_dev.join(itemItem_sim).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))

    val userItem_item_rant_dev_itemsim = itemItem_user_rant_dev_itemsim.map(f => ((f._2._1, f._1._1), (f._1._2, f._2._2, f._2._3, f._2._4)))
    val userItem_useritemsim = userItemSim.map(f => ((f._1, f._2), f._3))
    val userItem_item_rant_dev_itemsim_useritemsim = userItem_item_rant_dev_itemsim.join(userItem_useritemsim).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2)))


    val molecule = userItem_item_rant_dev_itemsim_useritemsim.map(f => {
      val value = (f._2._2 + f._2._3) * f._2._4 * f._2._5
      (f._1, value)
    }).reduceByKey(_ + _)

    val denominator = userItem_item_rant_dev_itemsim_useritemsim.map(f => {
      val value = f._2._4 * f._2._5
      (f._1,value)
    }).reduceByKey(_+_)

    val result = molecule.join(denominator).map(f=>(f._1,f._2._1/f._2._2)).map(f=>(f._1._1,f._1._2,f._2))
    result
  }
}
