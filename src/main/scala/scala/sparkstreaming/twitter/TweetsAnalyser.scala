package scala.sparkstreaming.twitter

import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.sentiment.SentimentUtils

import scala.sparkstreaming.twitter.SparkPopularHashTags.getClass

object TweetsAnalyser {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - Sentiment Analysis")
    val sc = new SparkContext(conf)

    def main(args: Array[String]) {

        sc.setLogLevel("WARN")

        val line = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/twitterconfigs.txt")).getLines

        for(l<- line){
            var arguments = l.split(":")
            System.setProperty("twitter4j.oauth."+arguments(0).trim,arguments(1).trim)
        }

        val ssc = new StreamingContext(sc, Seconds(5))

        val stream = TwitterUtils.createStream(ssc, None)

        val hashTags1 = stream.flatMap(status => status.getHashtagEntities)

        val h = stream.map(status=>(status.getText,status.getHashtagEntities))

        val hashTags = h.flatMap{case(txt,arr)=>arr.map(hash=>(hash,txt))}.groupByKey().map{case(e,s) => (e,s)}

        val trendingTags = hashTags.map((_, 1))
                .reduceByKeyAndWindow(_ + _, Minutes(5))
                .map { case (topic, count) => (count, topic) }
                .transform(_.sortByKey(false))

        trendingTags.foreachRDD(rdd => {
            val topList = rdd.take(10)
            println("\nTop hashtags in past 10 seconds (%s total):".format(rdd.count()))
            topList.foreach { case (count, tag) =>
                println("%s tweet (%s tag) (%s tagcount)".format(tag._2.mkString,tag._1.getText, count))
                val sentiment = SentimentAnalysisUtils.detectSentiment(tag._2.mkString)
                println(sentiment.toString)
            }
        }
        )

        ssc.start()
        ssc.awaitTermination()

    }
}