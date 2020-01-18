package com.fooww.research

import com.fooww.research.mae.MaeScala
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author ：zwy
 */
object Main {

  def main(args: Array[String]): Unit = {

    val trainLines = readFromTxtByLine("C:\\Users\\fooww\\Desktop\\数据\\数据\\train-80k.csv")
    val testLines = readFromTxtByLine("C:\\Users\\fooww\\Desktop\\数据\\数据\\test-20k.csv")

    val session = SparkSession.builder().appName("slope").master("local").getOrCreate()
    val sc = session.sparkContext

    val trainRDD: RDD[(Long, Long, Double)] = sc.parallelize(trainLines).map(_.split(",")).map(x => (x(0).toLong, x(1).toLong, x(2).toDouble))
    val testRDD: RDD[(Long, Long, Double)] = sc.parallelize(testLines).map(_.split(",")).map(x => (x(0).toLong, x(1).toLong, x(2)
      .toDouble))
    val slopeRecommendRDD = Recommend.getSlopeRDD(trainRDD)

    val mae = MaeScala.getMaeRddZip(testRDD,slopeRecommendRDD)
    print(mae)

    //    val slopeMae = MAE.getMae(slopeRecommendRDD,rantRDD,session)
    //    println("slope mae:"+slopeMae)
  }

  def readFromTxtByLine(filePath: String): Array[String] = {
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
