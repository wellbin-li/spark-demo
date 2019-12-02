package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建SparkConf对象
    //设定spark计算框架的运行（部署环境）
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //创建RDD
    //1)从内存中创建makeRDD，底层实现就是
    val listRDD = sc.makeRDD(List(1,2,3,4))
    //val arrayRDD = sc.makeRDD(Array("1","2","3"))

    //2)从内存中创建parallelize
    //val arrayRDD = sc.parallelize(Array(1,2,3,4))

    //3)从外部存储中创建
    //默认情况下，可以读取项目路径，也可以读取其他路径，HDFS
    //默认从文件中读取的数据都是字符串类类型
    //val fileRDD = sc.textFile("in")

    listRDD.collect().foreach(println)

    //将RDD的数据保存到文件中
    listRDD.saveAsTextFile("output")

  }

}
