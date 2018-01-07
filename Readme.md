
This is a simple example using Spark Streaming to analysis Twitter data in scala. The examples are on real time data and are easy to understand.

TweetsAnalyser : This class gets trending hashtags on twitter in the past 5 minutes.
SparkPopularHashTags : This class gets the sentiment of trending hashtags in the past 5 minutes using stanford NLP module.

Set up and installation :

This is a gradle project, make sure gradle environment is set up. Checkout the project and run the examples.

Few troubleshootings :

1. Make sure the versions of spark streaming and twitter module are correct. As the versions (>1.6.2) of twitter streaming module are not compatible with higher versions of spark streaming
