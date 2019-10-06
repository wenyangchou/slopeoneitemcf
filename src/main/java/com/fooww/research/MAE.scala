package com.fooww.research

import org.apache.spark.rdd.RDD

/**
  * @author ：zwy
  */
object MAE {

  def getMae(trainRDD:RDD[(Long,Long,Double)],testRDD:RDD[(Long,Long,Int)])={

    val train = trainRDD.map(f=>{
      ((f._1,f._2),f._3)
    })

    val test = testRDD.map(f=>{
      ((f._1,f._2),f._3)
    })

    val subRDD = test.join(train).map(f=>{
      (f._1,Math.pow(f._2._1 - f._2._2,2) )
    })

    var count = 0
    var mae:Double = 0

    subRDD.foreachPartition(partition=>{
      partition.foreach(f=>{
        count = count+1
      })
    })

    subRDD.foreachPartition(partition=>{
      partition.foreach(f=>{
        mae = mae + f._2 /count
      })
    })

    mae
  }

}
