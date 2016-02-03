TO RUN
-----------
Build with maven  - "mvn package"

Export ${SPARK_PROJ} (or replace) in following command with the absolute path to the spark git repository - for me this is /home/training/Documents/Spark_Attack/

Replace the 5 arguements in <...> with actual values

Then simply run:


spark-submit --master local[4] \
--driver-class-path ${SPARK_PROJ}/spark-project/src/lib/twitter4j-core-3.0.3.jar:${SPARK_PROJ}/spark-project/src/lib/twitter4j-stream-3.0.3.jar:${SPARK_PROJ}/spark-project/src/lib/spark-streaming-twitter_2.10-1.5.1.jar \
--class scala.streaming.TopWords ${SPARK_PROJ}/spark-project/target/spark-examples_2.10-1.5.1.jar \
<consumer key> <consumer secret> <access token> <access token secret> <search-term>
