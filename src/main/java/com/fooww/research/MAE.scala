package com.fooww.research

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author ：zwy
  */
object MAE {

  def getMae(trainRDD:RDD[(Long,Long,Double)],testRDD:RDD[(Long,Long,Double)],session:SparkSession)={

    val train = trainRDD.map(f=>{
      ((f._1,f._2),f._3)
    })

    val test = testRDD.map(f=>{
      ((f._1,f._2),f._3)
    })

    val subRDD = test.join(train).map(f=>{
      (f._1,Math.abs(f._2._1 - f._2._2) )
    })

    val df = session.createDataFrame(subRDD)
    val result = df.agg(("_2","avg")).collect()(0).get(0)
    result
  }

}
