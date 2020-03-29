package com.test

import scala.io.Source.fromURL
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object scalaOject extends App {
  
    val conf = new SparkConf().setAppName("Spark Streaming - Kafka Producer - PopularHashTags").set("spark.executor.memory", "1g")
  conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
  val sc = new SparkContext(conf)

  case class employee(first_name: String, last_name: String)

  val ssc = new StreamingContext(sc, Seconds(4))

/*  val wordStream = kafkaConsumeStream(ssc)

  wordStream.foreachRDD { rdd =>
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val partitioned_rdd = rdd.map(line => line.toString.split(",").map(_.trim))
    val df = partitioned_rdd.map { case Array(s0, s1) => employee(s0, s1) }.toDF

    df.show

    import org.elasticsearch.spark.sql._
    df.saveToEs("kafkawordcount_v1/kwc")
  }*/
}