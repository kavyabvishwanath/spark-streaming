package scala.sparkstreaming.twitter


import java.io.InputStream

import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object SparkPopularHashTags {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - Trending HashTags")
    val sc = new SparkContext(conf)

    def main(args: Array[String]) {

        sc.setLogLevel("WARN")

        /* Read config from config file in resource directory */
        val line = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/twitterconfigs.txt")).getLines

        for(l<- line){
            var arguments = l.split(":")
            System.setProperty("twitter4j.oauth."+arguments(0).trim,arguments(1).trim)
        }

        val ssc = new StreamingContext(sc, Seconds(10))

        val stream = TwitterUtils.createStream(ssc, None)

        val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

        val trendingTags = hashTags.map((_, 1))
                .reduceByKeyAndWindow(_ + _, Minutes(5))
                .map { case (topic, count) => (count, topic) }
                .transform(_.sortByKey(false))


        trendingTags.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nPopular topics in last 5 Minutes (%s total):".format(rdd.count()))
            topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
        }
        )

        ssc.start()
        ssc.awaitTermination()

    }
}