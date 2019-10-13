package com.fooww.research

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author ：zwy
  */
object Main {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("slope").master("local").getOrCreate()
    val sc = session.sparkContext
    val rantRDD:RDD[(Long,Long,Int)] = sc.textFile("C:\\Users\\Administrator\\Desktop\\rant.txt").map(_.split(",")).map(x=>(x(0).toLong,x(1).toLong,x(2).toInt))

    session.createDataFrame(rantRDD).show()

//    val slopeRecommendRDD = Recommend.getSlopeRDD(rantRDD)
//    val slopeMae = MAE.getMae(slopeRecommendRDD,rantRDD)
//    print(slopeMae)

//    val cfRecommendRDD = Recommend.getCFRDD(rantRDD)
//    val cfMae = MAE.getMae(cfRecommendRDD,rantRDD)
//    print(cfMae)

  }
}
