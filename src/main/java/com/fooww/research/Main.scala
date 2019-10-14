package com.fooww.research

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author ：zwy
  */
object Main {

  def main(args: Array[String]): Unit = {

    val lines = readFromTxtByLine("C:\\Users\\fooww\\Desktop\\ee.txt")

    val session = SparkSession.builder().appName("slope").master("local").getOrCreate()
    val sc = session.sparkContext

    val rantRDD:RDD[(Long,Long,Double)] = sc.parallelize(lines).map(_.split(",")).map(x=>(x(0).toLong,x(1).toLong,x(2).toDouble))
    val slopeRecommendRDD = Recommend.getSlopeRDD(rantRDD)
    val slopeMae = MAE.getMae(slopeRecommendRDD,rantRDD,session)

    val cfRecommendRDD = Recommend.getCFRDD(rantRDD)
    val cfMae = MAE.getMae(cfRecommendRDD,rantRDD,session)
    println("slope mae:"+slopeMae)
    println("cf mae:"+cfMae)
  }

  def readFromTxtByLine(filePath:String): Array[String] = {
    //导入Scala的IO包
    import scala.io.Source
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    val source = Source.fromFile(filePath, "UTF-8")
    //将所有行放到数组中
    val lines = source.getLines().toArray
    source.close()
    lines
  }

}
