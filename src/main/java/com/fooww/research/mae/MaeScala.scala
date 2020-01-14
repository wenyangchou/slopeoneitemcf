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

  def getMaeInt(observe:RDD[Int],predict:RDD[Int]):Double={
    val observePredict = observe.zip(predict)
    val maeAbs = observePredict.map(f=> Math.abs(f._1-f._2))
    maeAbs.sum()/maeAbs.count()
  }

  def getMaeFloat(observe:RDD[Float],predict:RDD[Float]):Double={
    val observePredict = observe.zip(predict)
    val maeAbs = observePredict.map(f=> Math.abs(f._1-f._2))
    maeAbs.sum()/maeAbs.count()
  }

  def getMaeDouble(observe:RDD[Double],predict:RDD[Double]):Double={
    val observePredict = observe.zip(predict)
    val maeAbs = observePredict.map(f=> Math.abs(f._1-f._2))
    maeAbs.sum()/maeAbs.count()
  }

  def getMaeRddJoin(observe:RDD[(Long,Long,Double)],predict:RDD[(Long,Long,Double)]):Double={
    val observeRdd = observe.map(f=>((f._1,f._2),f._3))
    val predictRdd = predict.map(f=>((f._1,f._2),f._3))

    val maeRdd = observeRdd.join(predictRdd)
    val maeAbs = maeRdd.map(f=>Math.abs(f._2._1 - f._2._2))
    maeAbs.sum()/maeAbs.count()
  }

  def getMaeRddZip(observe:RDD[(Long,Long,Double)],predict:RDD[(Long,Long,Double)]):Double={
    val maeRdd = observe.zip(predict)
    val maeAbs = maeRdd.map(f=>Math.abs(f._1._3 - f._2._3))
    maeAbs.sum()/maeAbs.count()
  }

}
