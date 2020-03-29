package com.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka._

object KafkaSparkPopularHashTags {

  val conf = new SparkConf().setAppName("Spark Streaming - Kafka Producer - PopularHashTags").set("spark.executor.memory", "1g")
  conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    sc.setLogLevel("WARN")

    // Create an array of arguments: zookeeper hostname/ip,consumer group, topicname, num of threads
    val Array(zkQuorum, group, topics, numThreads) = args

    // Set the Spark StreamingContext to create a DStream for every 2 seconds
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Map each topic to a thread
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    // Filter hashtags

    case class tags(topic: String,count:Int)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    lines.foreachRDD ({rdd => val df = rdd.toDF()
/*         val partitioned_rdd = rdd.map(_.split(" ").map((_ , 1)))
    val df = partitioned_rdd.map { case => tags(topic, count) }.toDF
      df*/.show()
    })

    /* wordStream.foreachRDD { rdd =>
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val partitioned_rdd = rdd.map(line => line.toString.split(",").map(_.trim))
    val df = partitioned_rdd.map { case Array(s0, s1) => employee(s0, s1) }.toDF

    df.show

    import org.elasticsearch.spark.sql._
    df.saveToEs("kafkawordcount_v1/kwc")
  }*/

    ssc.start()
    ssc.awaitTermination()

  }

}