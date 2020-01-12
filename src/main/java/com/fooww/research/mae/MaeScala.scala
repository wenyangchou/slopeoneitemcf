package com.fooww.research.mae

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * author:zwy
  * Date:2020-01-12  
  * Time:16:33  
  *
  *
  **/
object MaeScala {

  def getMae(observe:RDD[Int],predict:RDD[Int],session:SparkSession):Float={

    val observePredict = observe.zip(predict)
    val maeAbs = observePredict.map(f=> Math.abs(f._1-f._2))
    val mae = maeAbs

  }

}
